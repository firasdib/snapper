import psutil
import subprocess
import json
import os
import logging, logging.handlers
import re
import requests
import argparse
from datetime import datetime
from reports.email_report import create_email_report
from reports.discord_report import create_discord_report
from utils import format_delta, get_relative_path

#
# Read config

with open(get_relative_path(__file__, './config.json'), 'r') as f:
    config = json.load(f)


#
# Configure logging

def setup_logger(name, log_file, level='INFO'):
    if not os.path.exists(config['log_dir']):
        os.makedirs(config['log_dir'])

    log_file_path = os.path.join(config['log_dir'], log_file)

    handler = logging.handlers.RotatingFileHandler(log_file_path,
                                                   backupCount=max(config['log_count'], 1))
    handler.setFormatter(logging.Formatter('[%(asctime)s] - [%(levelname)s] - %(message)s'))

    if os.path.isfile(log_file_path):
        handler.doRollover()

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.hasHandlers():
        logger.handlers.clear()

    logger.addHandler(handler)
    logger.propagate = False

    return logger


raw_log = setup_logger('snapper_raw', 'snapper_raw.log')
log = setup_logger('snapper', 'snapper.log')
log.handlers = raw_log.handlers + log.handlers
log.addHandler(logging.StreamHandler())

#
# Parse command line args

parser = argparse.ArgumentParser(description='SnapRAID execution wrapper')
parser.add_argument('-f', '--force',
                    help='Ignore any set thresholds or warnings and execute all jobs regardless',
                    action='store_true')
args = vars(parser.parse_args())

force_script_execution = args['force']


#
# Helpers

def notify_warning(message):
    log.error(message)
    send_email('WARNING! SnapRAID jobs unsuccessful', message)
    send_discord(f':warning: [**WARNING!**] {message}')


def send_discord(message, embeds=None):
    webhook_url = config['discord_webhook_url']

    if webhook_url is None:
        return

    if embeds is None:
        embeds = []

    data = {
        'content': message,
        'embeds': embeds,
        'username': 'Snapper',
    }

    result = requests.post(webhook_url, json=data)

    try:
        result.raise_for_status()

        log.info('Successfully posted message to discord')
    except requests.exceptions.HTTPError as err:
        raise ConnectionError('Unable to send message to discord') from err


def send_email(subject, message):
    log.info('Attempting to send email...')

    mail_bin = config['mail_bin']
    from_email = config['from_email']
    to_email = config['to_email']

    if not os.path.isfile(mail_bin):
        raise FileNotFoundError('Unable to find mail executable', mail_bin)

    result = subprocess.run([
        mail_bin,
        '-a', 'Content-Type: text/html',
        '-s', subject,
        '-r', from_email,
        to_email
    ], input=message, capture_output=True, text=True)

    if result.stderr:
        raise ConnectionError('Unable to send email', result.stderr)

    log.info(f'Successfully sent email to {to_email}')


def is_running():
    for process in psutil.process_iter(attrs=['name']):
        if process.name().lower() == 'snapraid':
            return True

    return False


def set_snapraid_priority():
    if not config['low_priority']:
        return

    # Setting nice is enough, as ionice follows that per the documentation here:
    # https://www.kernel.org/doc/Documentation/block/ioprio.txt
    #
    # The default class `IOPRIO_CLASS_BE` sets ionice as: `io_nice = (cpu_nice + 20) / 5.`
    # The default nice is 0, which sets ionice to 4.
    # We set nice to 10, which results in ionice of 6 - this way it's not entirely down prioritized.

    os.nice(10)


def run_snapraid(commands):
    snapraid_bin = config['snapraid_bin']

    if not os.path.isfile(snapraid_bin):
        raise FileNotFoundError('Unable to find SnapRAID executable', snapraid_bin)

    if is_running():
        raise ChildProcessError('SnapRAID already seems to be running, unable to proceed.')

    result = subprocess.run([snapraid_bin] + commands, capture_output=True, text=True,
                            preexec_fn=set_snapraid_priority)

    raw_log.info(result.stdout)

    if result.stderr:
        raw_log.error(result.stderr)

        # Remove all "acceptable" errors
        # If there are errors that are not caught here, they are considered critical.

        snapraid_errors = re.sub(
            r"^(?:WARNING! (?:With \d+ disks it's recommended to use \w+ parity levels|You cannot "
            r"modify data disk during a sync)|Rerun the sync command when finished|Missing file "
            r".+)\.[\r\n]*$",
            '', result.stderr, flags=re.IGNORECASE | re.MULTILINE)

        if snapraid_errors != '':
            raw_log.error(result.stderr)

            raise SystemError(f'''A critical SnapRAID error was encountered during command 
            "snapraid {' '.join(commands)}". Here are the first 100 characters:\n```\n
            {snapraid_errors[0:100]}\n```\n\nExecution has been halted.''')

    # diff returns code 2 if a sync is required
    if result.returncode != 0 and not (commands[0] == 'diff' and result.returncode == 2):
        raise SystemError(f'SnapRAID exited with code {result.returncode}, please review the logs.')

    return result.stdout, result.stderr


def get_status():
    snapraid_status, _ = run_snapraid(['status'])

    stats_regex = re.compile(
        r'^ +(?P<file_count>\d+) +(?P<fragmented_files>\d+) +(?P<excess_fragments>\d+) +('
        r'?P<wasted_gb>[-.\d]+) +(?P<used_gb>\d+) +(?P<free_gb>\d+) +(?P<use_percent>\d+)%(?: +('
        r'?P<drive_name>\S+)|$)',
        flags=re.MULTILINE)
    drive_stats = [m.groupdict() for m in stats_regex.finditer(snapraid_status)]

    scrub_info = re.search(
        r'scrubbed (?P<scrub_age>\d+) days ago, the median (?P<median>\d+), the newest ('
        r'?P<newest>\d+)',
        snapraid_status)
    unscrubbed_percent = re.search(
        r'^The (?P<not_scrubbed_percent>\d+)% of the array is not scrubbed', snapraid_status,
        flags=re.MULTILINE)
    error_count = re.search(r'^DANGER! In the array there are (?P<error_count>\d+) errors!',
                            snapraid_status, flags=re.MULTILINE)
    zero_subsecond_count = re.search(
        r'^You have (?P<touch_files>\d+) files with zero sub-second timestamp', snapraid_status,
        flags=re.MULTILINE)

    sync_in_progress = bool(
        re.search(r'^You have a sync in progress', snapraid_status, flags=re.MULTILINE))

    if scrub_info is None:
        raise ValueError('Unable to parse SnapRAID status')

    unscrubbed_percent = 0 if unscrubbed_percent is None else int(unscrubbed_percent[1])
    zero_subsecond_count = 0 if zero_subsecond_count is None else int(zero_subsecond_count[1])
    error_count = 0 if error_count is None else int(error_count[1])

    return (
        drive_stats,
        {
            'unscrubbed': unscrubbed_percent,
            'scrub_age': int(scrub_info[1]),
            'median': int(scrub_info[2]),
            'newest': int(scrub_info[3])
        },
        error_count,
        zero_subsecond_count,
        sync_in_progress
    )


def get_diff():
    snapraid_diff, _ = run_snapraid(['diff'])

    diff_regex = re.compile(r'''^ +(?P<equal>\d+) equal$
^ +(?P<added>\d+) added$
^ +(?P<removed>\d+) removed$
^ +(?P<updated>\d+) updated$
^ +(?P<moved>\d+) moved$
^ +(?P<copied>\d+) copied$
^ +(?P<restored>\d+) restored$''', flags=re.MULTILINE)
    [diff_data] = [m.groupdict() for m in diff_regex.finditer(snapraid_diff)]

    if diff_data is None:
        raise ValueError('Unable to parse diff output from SnapRAID, not proceeding.')

    diff_int = dict([a, int(x)] for a, x in diff_data.items())

    return diff_int


def get_smart():
    smart_data, _ = run_snapraid(['smart'])

    drive_regex = re.compile(r'^ +(?P<temp>\d+|-) +(?P<power_on_days>\d+|-) +('
                             r'?P<error_count>\d+|-) +(?P<fp>\d+%|-|SSD) +(?P<size>\S+) +('
                             r'?P<serial>\S+) +(?P<device>\S+) +(?P<disk>\S+)$', flags=re.MULTILINE)
    drive_data = [m.groupdict() for m in drive_regex.finditer(smart_data)]

    global_fp = re.search(r'next year is (?P<total_fp>\d+)%', smart_data).group(1)

    if drive_data is None or global_fp is None:
        raise ValueError('Unable to parse drive data or global failure percentage, not proceeding.')

    return drive_data, global_fp


def _run_sync(run_count):
    sync_output, sync_errors = run_snapraid(
        ['sync', '-h', '-q'] if config['prehash'] else ['sync', '-q'])

    check_completed_status(sync_output, 'SYNC')

    if re.search(r"^Rerun the sync command when finished", sync_errors,
                 flags=re.MULTILINE | re.IGNORECASE):
        log.info('SnapRAID has indicated another sync is recommended, likely due to files being '
                 'modified during the sync process.')

        if config['auto_resync']:
            if run_count > config['max_resync_attempts']:
                raise SystemError(
                    'Too many attempts to resync the array, manual intervention recommended.')

            log.info('Re-running sync command with identical options...')
            _run_sync(run_count + 1)


def run_sync():
    start = datetime.now()
    _run_sync(1)
    end = datetime.now()

    return end - start


def run_scrub():
    start = datetime.now()

    if config['scrub_new']:
        log.info('Scrubbing new blocks...')
        scrub_new_output, _ = run_snapraid(['scrub', '-p', 'new', '-q'])

        check_completed_status(scrub_new_output, 'SCRUB NEW')

    log.info('Scrubbing old blocks...')
    scrub_output, _ = run_snapraid(
        ['scrub', '-p', str(config['scrub_percent']), '-o', str(config['scrub_age']), '-q'])

    end = datetime.now()

    check_completed_status(scrub_output, 'SCRUB')

    return end - start


def run_touch():
    run_snapraid(['touch'])


def check_completed_status(message, job_type):
    if not re.search(r'^Everything OK', message, flags=re.MULTILINE) and not re.search(
            r'^Nothing to do', message, flags=re.MULTILINE):
        raise SystemError(f'SnapRAID {job_type} job did not finish as expected, please check your '
                          f'logs. Remaining jobs have been cancelled.')


def sanity_check():
    config_file = config['snapraid_config_file']

    if not os.path.isfile(config_file):
        raise FileNotFoundError('Unable to find SnapRAID configuration', config_file)

    with open(config_file, 'r') as file:
        config_content = file.read()

    file_regex = re.compile(r'^(?:content|parity) +(.+/snapraid.(?:content|parity)) *$',
                            flags=re.MULTILINE)
    files = [m[1] for m in file_regex.finditer(config_content)]

    for file in files:
        if not os.path.isfile(file):
            raise FileNotFoundError('Unable to locate required content/parity file', file)

    log.info(f'All {len(files)} content and parity files found, proceeding.')


#
# Main

def main():
    try:
        # Metadata for report
        sync_job_time = None
        scrub_job_time = None
        sync_job_ran = False
        scrub_job_ran = False

        total_start = datetime.now()

        log.info('Snapper started')
        log.info('Running sanity checks...')

        if not force_script_execution:
            sanity_check()

        log.info('Checking for errors and files with zero sub-second timestamps...')

        (_, _, error_count, zero_subsecond_count, sync_in_progress) = get_status()

        if error_count > 0 and not force_script_execution:
            raise SystemError(f'There are {error_count} errors in you array, you should review '
                              f'this immediately. All jobs have been halted.')

        if zero_subsecond_count > 0:
            log.info(f'Found {zero_subsecond_count} file(s) with zero sub-second timestamp')
            log.info('Running touch job...')
            run_touch()

        log.info('Get SnapRAID diff...')

        diff_data = get_diff()
        added_threshold = config['added_threshold']
        removed_threshold = config['removed_threshold']

        log.info(f'Diff output: {diff_data["equal"]} equal, ' +
                 f'{diff_data["added"]} added, ' +
                 f'{diff_data["removed"]} removed, ' +
                 f'{diff_data["updated"]} updated, ' +
                 f'{diff_data["moved"]} moved, ' +
                 f'{diff_data["copied"]} copied, ' +
                 f'{diff_data["restored"]} restored')

        if sum(diff_data.values()) - diff_data["equal"] > 0 or sync_in_progress or force_script_execution:
            if force_script_execution:
                log.info('Ignoring any thresholds and forcefully proceeding with sync.')
            elif 0 < added_threshold < diff_data["added"]:
                raise ValueError(
                    f'More files ({diff_data["added"]}) have been added than the configured max ({added_threshold})')
            elif 0 < removed_threshold < diff_data["removed"]:
                raise ValueError(
                    f'More files ({diff_data["removed"]}) have been removed than the configured '
                    f'max ({removed_threshold})')
            elif sync_in_progress:
                log.info('A previous sync in progress has been detected, resuming.')
            else:
                log.info(f'Fewer files added ({diff_data["added"]}) than the configured limit ({added_threshold}), proceeding.')
                log.info(f'Fewer files removed ({diff_data["removed"]}) than the configured limit ({removed_threshold}), proceeding.')

            log.info(f'Running SnapRAID sync {"with" if config["prehash"] else "without"} pre'
                     f'-hashing...')
            elapsed_time = run_sync()
            sync_job_time = format_delta(elapsed_time)
            log.info(f'Sync job finished, elapsed time {sync_job_time}')

            sync_job_ran = True
        else:
            log.info('No changes to sync, skipping.')

        if config['scrub_percent'] > 0:
            log.info('Running scrub job...')
            elapsed_time = run_scrub()
            scrub_job_time = format_delta(elapsed_time)
            log.info(f'Scrub job finished, elapsed time {scrub_job_time}')

            scrub_job_ran = True
        else:
            log.info('Scrubbing not enabled, skipping.')

        log.info('Fetching SnapRAID status...')
        (drive_stats, scrub_stats, error_count, _, _) = get_status()

        log.info(f'{scrub_stats["unscrubbed"]}% of the array has not been scrubbed, with the '
                 f'oldest block at {scrub_stats["scrub_age"]} day(s), the '
                 f'median at {scrub_stats["median"]} day(s), and the newest at '
                 f'{scrub_stats["newest"]} day(s).')

        log.info('Fetching smart data...')
        (smart_drive_data, global_fp) = get_smart()

        log.info(f'Drive failure probability this year is {global_fp}%')

        total_time = format_delta(datetime.now() - total_start)

        email_report = create_email_report(
            sync_job_ran,
            scrub_job_ran,
            sync_job_time,
            scrub_job_time,
            diff_data,
            zero_subsecond_count,
            scrub_stats,
            drive_stats,
            smart_drive_data,
            global_fp,
            total_time
        )

        send_email('SnapRAID Job Completed Successfully', email_report)

        if not config['discord_webhook_url'] is None:
            (discord_message, embeds) = create_discord_report(
                sync_job_ran,
                scrub_job_ran,
                sync_job_time,
                scrub_job_time,
                diff_data,
                zero_subsecond_count,
                scrub_stats,
                drive_stats,
                smart_drive_data,
                global_fp,
                total_time
            )

            send_discord(discord_message, embeds)

        log.info('SnapRAID jobs completed successfully, exiting.')
    except (ValueError, ChildProcessError, SystemError) as err:
        notify_warning(err.args[0])
    except ConnectionError as err:
        log.error(str(err))
    except FileNotFoundError as err:
        notify_warning(f'{err.args[0]} - missing file path `{err.args[1]}`')


main()
