import argparse
import concurrent.futures
import gzip
import json
import logging
import logging.handlers
import math
import os
import re
import shutil
import subprocess
import traceback
from datetime import datetime, timedelta
from operator import itemgetter

import psutil
import requests
import pidfile
from jsonschema import validate

from reports.discord_report import create_discord_report
from reports.email_report import create_email_report
from utils import format_delta, get_relative_path

#
# Read config

with open(get_relative_path(__file__, './config.json'), 'r') as f:
    config = json.load(f)

with open(get_relative_path(__file__, './config.schema.json'), 'r') as f:
    schema = json.load(f)

validate(instance=config, schema=schema)


#
# Configure logging

def rotator(source, dest):
    with open(source, 'rb') as f_in:
        with gzip.open(dest, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    os.remove(source)


def setup_logger(name, log_file, level='INFO'):
    log_dir, max_count = itemgetter('dir', 'max_count')(config['logs'])

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_file_path = os.path.join(log_dir, log_file)
    needs_rollover = os.path.isfile(log_file_path)

    handler = logging.handlers.RotatingFileHandler(log_file_path, backupCount=max(max_count, 1))
    handler.setFormatter(logging.Formatter('[%(asctime)s] - [%(levelname)s] - %(message)s'))

    handler.rotator = rotator
    handler.namer = lambda file_name: file_name + '.gz'

    if needs_rollover:
        handler.doRollover()

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.hasHandlers():
        logger.handlers.clear()

    logger.addHandler(handler)
    logger.propagate = False

    return logger


#
# Parse command line args

parser = argparse.ArgumentParser(description='SnapRAID execution wrapper')
parser.add_argument('-f', '--force',
                    help='Ignore any set thresholds or warnings and execute all jobs regardless',
                    action='store_true')
args = vars(parser.parse_args())

force_script_execution = args['force']


#
# Notification helpers

def notify_and_handle_error(message, error):
    log.error(message)
    log.error(''.join(traceback.format_exception(None, error, error.__traceback__)))

    send_email('WARNING! SnapRAID jobs unsuccessful', message.replace('\n', '<br>'))
    notify_warning(message)

    exit(1)


def notify_warning(message, embeds=None):
    return send_discord(f':warning: [**WARNING!**] {message}', embeds=embeds)


def notify_info(message, embeds=None, message_id=None):
    return send_discord(f':information_source: [**INFO**] {message}', embeds, message_id)


def send_discord(message, embeds=None, message_id=None):
    is_enabled, webhook_id, webhook_token = itemgetter(
        'enabled', 'webhook_id', 'webhook_token')(config['notifications']['discord'])

    if not is_enabled:
        return

    if embeds is None:
        embeds = []

    data = {
        'content': message,
        'embeds': embeds,
        'username': 'Snapper',
    }

    update_message = message_id is not None
    base_url = f'https://discord.com/api/webhooks/{webhook_id}/{webhook_token}'

    if update_message:
        discord_url = f'{base_url}/messages/{message_id}'
        response = requests.patch(discord_url, json=data)
    else:
        discord_url = f'{base_url}?wait=true'
        response = requests.post(discord_url, json=data)

    try:
        response.raise_for_status()
        log.debug('Successfully posted message to discord')

        if not update_message:
            data = response.json()

            # Return the message ID in case we want to manipulate it
            return data['id']
    except requests.exceptions.HTTPError as err:
        # Handle error when trying to update a message
        if update_message:
            log.debug('Failed to update message, posting new.')
            return send_discord(message, embeds=embeds)

        log.error('Unable to send message to discord')
        log.error(str(err))


def send_email(subject, message):
    log.debug('Attempting to send email...')

    is_enabled, mail_bin, from_email, to_email = itemgetter(
        'enabled', 'binary', 'from_email', 'to_email')(config['notifications']['email'])

    if not is_enabled:
        return

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

    log.debug(f'Successfully sent email to {to_email}')


#
# Snapraid Helpers

def is_running():
    for process in psutil.process_iter(attrs=['name']):
        if process.name().lower() == 'snapraid':
            return True

    return False


def set_snapraid_priority():
    # Setting nice is enough, as ionice follows that per the documentation here:
    # https://www.kernel.org/doc/Documentation/block/ioprio.txt
    #
    # The default class `IOPRIO_CLASS_BE` sets ionice as: `io_nice = (cpu_nice + 20) / 5.`
    # The default nice is 0, which sets ionice to 4.
    # We set nice to 10, which results in ionice of 6 - this way it's not entirely down prioritized.

    nice_level = config['snapraid']['nice']
    os.nice(nice_level)
    p = psutil.Process(os.getpid())
    p.ionice(psutil.IOPRIO_CLASS_BE, math.floor((nice_level + 20) / 5))


def spin_down():
    hdparm_bin, is_enabled, drives = itemgetter('binary', 'enabled', 'drives')(config['spindown'])

    if not is_enabled:
        return

    if not os.path.isfile(hdparm_bin):
        raise FileNotFoundError('Unable to find hdparm executable', hdparm_bin)

    log.info(f'Attempting to spin down all {drives} drives...')

    content_files, parity_files = get_snapraid_config()
    drives_to_spin_down = parity_files + (content_files if drives == 'all' else [])

    shell_command = (f'{hdparm_bin} -y $('
                     f'df {" ".join(drives_to_spin_down)} | '  # Get the drives
                     f'tail -n +2 | '  # Remove the header
                     f'cut -d " " -f1 | '  # Split by space, get the first item
                     f'tr "\\n" " "'  # Replace newlines with spaces
                     f')')

    try:
        process = subprocess.run(shell_command, shell=True, capture_output=True, text=True)

        rc = process.returncode

        if rc == 0:
            log.info('Successfully spun down drives.')
        else:
            log.error(f'Unable to successfully spin down hard drives, see error output below.')
            log.error(process.stderr)
            log.error(f'Shell command executed: {shell_command}')
    except Exception as err:
        log.error(f'Encountered exception while attempting to spin down drives:')
        log.error(str(err))


#
# Snapraid Commands

def run_snapraid(commands, progress_handler=None, allowed_return_codes=[]):
    snapraid_bin, snapraid_config = itemgetter('binary', 'config')(config['snapraid'])

    if not os.path.isfile(snapraid_bin):
        raise FileNotFoundError('Unable to find SnapRAID executable', snapraid_bin)

    if is_running():
        raise ChildProcessError('SnapRAID already seems to be running, unable to proceed.')

    std_out = []
    std_err = []

    with (subprocess.Popen(
            [snapraid_bin, '--conf', snapraid_config] + commands,
            shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            preexec_fn=set_snapraid_priority, encoding="utf-8",
            errors='replace'
    ) as process,
        concurrent.futures.ThreadPoolExecutor(2) as tpe,
    ):
        def read_stdout(file):
            for line in file:
                rline = line.rstrip()

                raw_log.info(rline)

                if progress_handler is None or not progress_handler(rline):
                    std_out.append(rline)

        def read_stderr(file):
            for line in file:
                rline = line.rstrip()

                raw_log.error(rline)
                std_err.append(rline)

        f1 = tpe.submit(read_stdout, process.stdout)
        f2 = tpe.submit(read_stderr, process.stderr)
        f1.result()
        f2.result()

    rc = process.returncode

    if not (rc == 0 or rc in allowed_return_codes):
        last_lines = '\n'.join(std_err[-10:])

        raise SystemError(f'A critical SnapRAID error was encountered during command '
                          f'`snapraid {" ".join(commands)}`. The process exited with code `{rc}`.\n'
                          f'Here are the last **10 lines** from the error log:\n```\n'
                          f'{last_lines}\n```\nThis requires your immediate attention.',
                          '\n'.join(std_err))

    return '\n'.join(std_out), '\n'.join(std_err)


def get_status():
    snapraid_status, _ = run_snapraid(['status'])

    stats_regex = re.compile(
        r'^ *(?P<file_count>\d+) +(?P<fragmented_files>\d+) +(?P<excess_fragments>\d+) +('
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
    snapraid_diff, _ = run_snapraid(['diff'], allowed_return_codes=[2])

    diff_regex = re.compile(r'''^ *(?P<equal>\d+) equal$
^ *(?P<added>\d+) added$
^ *(?P<removed>\d+) removed$
^ *(?P<updated>\d+) updated$
^ *(?P<moved>\d+) moved$
^ *(?P<copied>\d+) copied$
^ *(?P<restored>\d+) restored$''', flags=re.MULTILINE)

    diff_data = [m.groupdict() for m in diff_regex.finditer(snapraid_diff)]

    if len(diff_data) == 0:
        raise ValueError('Unable to parse diff output from SnapRAID, not proceeding.')

    diff_int = dict([a, int(x)] for a, x in diff_data[0].items())

    return diff_int


def get_smart():
    smart_data, _ = run_snapraid(['smart'])

    drive_regex = re.compile(r'^ *(?P<temp>\d+|-) +(?P<power_on_days>\d+|-) +('
                             r'?P<error_count>\d+|-) +(?P<fp>\d+%|-|SSD) +(?P<size>\S+) +('
                             r'?P<serial>\S+) +(?P<device>\S+) +(?P<disk>\S+)$', flags=re.MULTILINE)
    drive_data = [m.groupdict() for m in drive_regex.finditer(smart_data)]

    global_fp = re.search(r'next year is (?P<total_fp>\d+)%', smart_data).group(1)

    if drive_data is None or global_fp is None:
        raise ValueError('Unable to parse drive data or global failure percentage, not proceeding.')

    return drive_data, global_fp


def handle_progress():
    start = datetime.now()
    message_id = None

    def handler(data):
        nonlocal start
        nonlocal message_id

        progress_data = re.search(r'^(?P<progress>\d+)%, (?P<progress_mb>\d+) MB'
                                  r'(?:, (?P<speed_mb>\d+) MB/s, (?P<speed_stripe>\d+) stripe/s, '
                                  r'CPU (?P<cpu>\d+)%, (?P<eta_h>\d+):(?P<eta_m>\d+) ETA)?$', data,
                                  flags=re.MULTILINE)

        is_progress = bool(progress_data)

        if is_progress and datetime.now() - start >= timedelta(minutes=1):
            msg = f'Current progress **{progress_data.group(1)}%** (`{progress_data.group(2)} MB`)'

            if progress_data.group(3) is not None:
                msg = (f'{msg} — processing at **{progress_data.group(3)} MB/s** '
                       f'(*{progress_data.group(4)} stripe/s, {progress_data.group(5)}% CPU*). '
                       f'**ETA:** {progress_data.group(6)}h {progress_data.group(7)}m')

            if message_id is None:
                message_id = notify_info(msg)
            else:
                new_message_id = notify_info(msg, message_id=message_id)

                if new_message_id:
                    message_id = new_message_id

            start = datetime.now()

        return is_progress

    return handler


def _run_sync(run_count):
    pre_hash, auto_sync = itemgetter('pre_hash', 'auto_sync')(config['snapraid']['sync'])
    auto_sync_enabled, max_attempts = itemgetter('enabled', 'max_attempts')(auto_sync)

    try:
        log.info(f'Running SnapRAID sync ({run_count}) '
                 f'{"with" if pre_hash else "without"} pre-hashing...')
        notify_info(f'Syncing **({run_count})**...')

        run_snapraid(['sync', '-h'] if pre_hash else ['sync'], handle_progress())
    except SystemError as err:
        sync_errors = err.args[1]

        if sync_errors is None:
            raise err

        # The three errors in the regex are considered "safe", i.e.,
        # a file was just modified or removed during the sync.
        #
        # This is normally nothing to be worried about, and the operation can just be rerun.
        # If there are other errors in the output, and not only these, we don't want to re-run
        # the sync command, because it could be things we need to have a closer look at.

        bad_errors = re.sub(r'^(?:'
                            r'WARNING! You cannot modify (?:files|data disk) during a sync|'
                            r'Unexpected (?:time|size) change at file .+|'
                            r'Missing file .+|'
                            r'Rerun the sync command when finished|'
                            r'WARNING! With \d+ disks it\'s recommended to use \w+ parity levels|'
                            r'WARNING! Unexpected file errors!'
                            r')\.\s*',
                            '', sync_errors, flags=re.MULTILINE | re.IGNORECASE)
        should_rerun = bad_errors == '' and re.search(r'^Rerun the sync command when finished',
                                                      sync_errors,
                                                      flags=re.MULTILINE | re.IGNORECASE)

        if should_rerun:
            log.info(
                'SnapRAID has indicated another sync is recommended, due to disks or files being '
                'modified during the sync process.')

        if should_rerun and auto_sync_enabled and run_count < max_attempts:
            log.info('Re-running sync command with identical options...')
            _run_sync(run_count + 1)
        else:
            raise err


def run_sync():
    start = datetime.now()
    _run_sync(1)
    end = datetime.now()

    sync_job_time = format_delta(end - start)

    log.info(f'Sync job finished, elapsed time {sync_job_time}')
    notify_info(f'Sync job finished, elapsed time **{sync_job_time}**')

    return sync_job_time


def run_scrub():
    enabled, scrub_new, check_percent, min_age = itemgetter(
        'enabled', 'scrub_new', 'check_percent', 'min_age')(config['snapraid']['scrub'])

    if not enabled:
        log.info('Scrubbing not enabled, skipping.')

        return None

    log.info('Running scrub job...')

    start = datetime.now()

    if scrub_new:
        log.info('Scrubbing new blocks...')
        notify_info('Scrubbing new blocks...')

        scrub_new_output, _ = run_snapraid(['scrub', '-p', 'new'], handle_progress())

    log.info('Scrubbing old blocks...')
    notify_info('Scrubbing old blocks...')

    scrub_output, _ = run_snapraid(
        ['scrub', '-p', str(check_percent), '-o', str(min_age)],
        handle_progress())

    end = datetime.now()

    scrub_job_time = format_delta(end - start)

    log.info(f'Scrub job finished, elapsed time {scrub_job_time}')
    notify_info(f'Scrub job finished, elapsed time **{scrub_job_time}**')

    return scrub_job_time


def run_touch():
    run_snapraid(['touch'])


#
# Sanity Checker

def get_snapraid_config():
    config_file = config['snapraid']['config']

    if not os.path.isfile(config_file):
        raise FileNotFoundError('Unable to find SnapRAID configuration', config_file)

    with open(config_file, 'r') as file:
        snapraid_config = file.read()

    file_regex = re.compile(r'^(content|parity) +(.+/\w+.(?:content|parity)) *$',
                            flags=re.MULTILINE)
    parity_files = []
    content_files = []

    for m in file_regex.finditer(snapraid_config):
        if m[1] == 'content':
            content_files.append(m[2])
        else:
            parity_files.append(m[2])

    return content_files, parity_files


def sanity_check():
    content_files, parity_files = get_snapraid_config()
    files = content_files + parity_files

    for file in files:
        if not os.path.isfile(file):
            raise FileNotFoundError('Unable to locate required content/parity file', file)

    log.info(f'All {len(files)} content and parity files found, proceeding.')


#
# Main

def main():
    try:
        total_start = datetime.now()

        log.info('Snapper started')
        notify_info('Starting SnapRAID jobs...')

        log.info('Running sanity checks...')

        sanity_check()

        log.info('Checking for errors and files with zero sub-second timestamps...')

        (_, _, error_count, zero_subsecond_count, sync_in_progress) = get_status()

        if error_count > 0:
            if force_script_execution:
                log.error(f'There are {error_count} errors in you array, '
                          f'ignoring due to forced run.')
                notify_warning(f'There are **{error_count}** errors in you array, '
                               f'ignoring due to forced run.')
            else:
                raise SystemError(f'There are {error_count} errors in you array, you should review '
                                  f'this immediately. All jobs have been halted.')

        if zero_subsecond_count > 0:
            log.info(f'Found {zero_subsecond_count} file(s) with zero sub-second timestamp')
            log.info('Running touch job...')
            run_touch()

        log.info('Get SnapRAID diff...')

        diff_data = get_diff()

        log.info(f'Diff output: {diff_data["equal"]} equal, ' +
                 f'{diff_data["added"]} added, ' +
                 f'{diff_data["removed"]} removed, ' +
                 f'{diff_data["updated"]} updated, ' +
                 f'{diff_data["moved"]} moved, ' +
                 f'{diff_data["copied"]} copied, ' +
                 f'{diff_data["restored"]} restored')

        if sum(diff_data.values()) - diff_data["equal"] > 0 or sync_in_progress or \
                force_script_execution:
            updated_threshold, removed_threshold = itemgetter('updated', 'removed')(
                config['snapraid']['diff']['thresholds'])

            if force_script_execution:
                log.info('Ignoring any thresholds and forcefully proceeding with sync.')
            elif 0 < updated_threshold < diff_data["updated"]:
                raise ValueError(f'More files ({diff_data["updated"]}) have been updated than the '
                                 f'configured max ({updated_threshold})')
            elif 0 < removed_threshold < diff_data["removed"]:
                raise ValueError(
                    f'More files ({diff_data["removed"]}) have been removed than the configured '
                    f'max ({removed_threshold})')
            elif sync_in_progress:
                log.info('A previous sync in progress has been detected, resuming.')
            else:
                log.info(f'Fewer files updated ({diff_data["updated"]}) than the configured '
                         f'limit ({updated_threshold}), proceeding.')
                log.info(f'Fewer files removed ({diff_data["removed"]}) than the configured '
                         f'limit ({removed_threshold}), proceeding.')

            sync_job_time = run_sync()
            sync_job_ran = True
        else:
            log.info('No changes to sync, skipping.')
            notify_info('No changes to sync')

            sync_job_ran = False
            sync_job_time = None

        scrub_job_time = run_scrub()
        scrub_job_ran = scrub_job_time is not None

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

        report_data = {
            'sync_job_ran': sync_job_ran,
            'scrub_job_ran': scrub_job_ran,
            'sync_job_time': sync_job_time,
            'scrub_job_time': scrub_job_time,
            'diff_data': diff_data,
            'zero_subsecond_count': zero_subsecond_count,
            'scrub_stats': scrub_stats,
            'drive_stats': drive_stats,
            'smart_drive_data': smart_drive_data,
            'global_fp': global_fp,
            'total_time': total_time
        }

        email_report = create_email_report(report_data)

        send_email('SnapRAID Job Completed Successfully', email_report)

        if config['notifications']['discord']['enabled']:
            (discord_message, embeds) = create_discord_report(report_data)

            send_discord(discord_message, embeds)

        spin_down()

        log.info('SnapRAID jobs completed successfully, exiting.')
    except (ValueError, ChildProcessError, SystemError) as err:
        notify_and_handle_error(err.args[0], err)
    except ConnectionError as err:
        log.error(str(err))
    except FileNotFoundError as err:
        notify_and_handle_error(f'{err.args[0]} - missing file path `{err.args[1]}`', err)
    except BaseException as err:
        notify_and_handle_error(
            f'Unhandled Python Exception `{str(err) if str(err) else "unknown error"}`', err)


try:
    with pidfile.PIDFile('/tmp/snapper.pid'):
        # Setup loggers after pidfile has been acquired
        raw_log = setup_logger('snapper_raw', 'snapper_raw.log')
        log = setup_logger('snapper', 'snapper.log')

        log.handlers = raw_log.handlers + log.handlers
        log.addHandler(logging.StreamHandler())

        main()
except pidfile.AlreadyRunningError:
    print('snapper already appears to be running!')
