import psutil
import subprocess
import json
import os
import logging, logging.handlers
import re
import requests
import argparse
from datetime import datetime
from email_report import create_email_report
from discord_report import create_discord_report
from utils import format_delta

#
# Read config

with open('config.json', 'r') as f:
    config = json.load(f)


#
# Configure logging

def setup_logger(name, log_file, level='INFO'):
    log_dir = os.path.abspath(config['log_dir'])

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_file_path = os.path.join(log_dir, log_file)

    handler = logging.handlers.RotatingFileHandler(log_file_path, backupCount=max(config['log_count'], 1))
    handler.setFormatter(logging.Formatter('[%(asctime)s] - [%(levelname)s] - %(message)s'))

    if os.path.isfile(log_file_path):
        handler.doRollover()

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if (logger.hasHandlers()):
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
parser.add_argument('-f', '--force', help='Ignore any set thresholds or warnings and execute all jobs regardless', action='store_true')
args = vars(parser.parse_args())

force_script_execution = args['force']

#
# Helpers

def notify_warning(message):
    message = message + ' Please review your logs ASAP.'
    send_email('WARNING! SnapRAID jobs unsuccessful', message)
    send_discord(':warning: ' + message)

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
    except requests.exceptions.HTTPError as err:
        log.error(f'Unable to send message to discord, {err}')
    else:
        log.info('Successfully posted message to discord')


def send_email(subject, message):
    log.info('Attempting to send email...')

    mail_bin = config['mail_bin']
    from_email = config['from_email']
    to_email = config['to_email']

    if not os.path.isfile(mail_bin):
        log.error(f'Unable to find mail executable at "{mail_bin}".')
        exit(1)

    result = subprocess.run([
        mail_bin,
        '-a', 'Content-Type: text/html',
        '-s', subject,
        '-r', from_email,
        to_email
    ], input=message, capture_output=True, text=True)

    if result.stderr:
        log.error(f'Unable to send email: {result.stderr}')
    else:
        log.info(f'Successfully sent email to {to_email}')


def is_running():
    for process in psutil.process_iter(attrs=['name']):
        if process.name().lower() == 'snapraid':
            return True

    return False


def run_snapraid(commands):
    snapraid_bin = config['snapraid_bin']

    if not os.path.isfile(snapraid_bin):
        msg = f'Unable to find SnapRAID executable at "{snapraid_bin}", unable to proceed.'
        log.error(msg)
        notify_warning(msg)
        exit(1)

    if is_running():
        msg = 'SnapRAID already seems to be running, unable to proceed.'
        log.error(msg)
        notify_warning(msg)
        exit(1)

    result = subprocess.run([snapraid_bin] + commands, capture_output=True, text=True)

    # Ignore fairly safe/common warnings
    if result.stderr and re.search(
            r"WARNING! +(?!With \d+ disks it's recommended to use \w+ parity levels|You cannot modify data disk during a sync)",
            result.stderr):
        msg = f'SnapRAID error during command "{commands}" - {result.stderr}. Execution has been halted.'
        log.error(msg)
        notify_warning(msg)

        raw_log.error(result.stderr)
        exit(1)

    raw_log.info(result.stdout)

    return result.stdout


def get_status():
    snapraid_status = run_snapraid(['status'])

    stats_regex = re.compile(
        r'^ +(?P<file_count>\d+) +(?P<fragmented_files>\d+) +(?P<excess_fragments>\d+) +(?P<wasted_gb>[-.\d]+) +(?P<used_gb>\d+) +(?P<free_gb>\d+) +(?P<use_percent>\d+)%(?: +(?P<drive_name>\S+)|(?P<global_stats>)$)',
        flags=re.MULTILINE)
    drive_stats = [m.groupdict() for m in stats_regex.finditer(snapraid_status)]

    scrub_info = re.search(
        r'scrubbed (?P<scrub_age>\d+) days ago, the median (?P<median>\d+), the newest (?P<newest>\d+)',
        snapraid_status)
    unscrubbed_percent = re.search(r'^The (?P<not_scrubbed_percent>\d+)% of the array is not scrubbed', snapraid_status,
                                   flags=re.MULTILINE)
    error_count = re.search(r'^DANGER! In the array there are (?P<error_count>\d+) errors!', snapraid_status,
                            flags=re.MULTILINE)
    zero_subsecond_count = re.search(r'^You have (?P<touch_files>\d+) files with zero sub-second timestamp',
                                     snapraid_status, flags=re.MULTILINE)

    if scrub_info is None:
        msg = 'Unable to parse SnapRAID status, not proceeding.'
        log.error(msg)
        notify_warning(msg)
        exit(1)

    if unscrubbed_percent is None:
        # 0% unscrubbed
        unscrubbed_percent = 0
    else:
        unscrubbed_percent = int(unscrubbed_percent[1])

    if zero_subsecond_count is None:
        zero_subsecond_count = 0
    else:
        zero_subsecond_count = int(zero_subsecond_count[1])

    if error_count is None:
        error_count = 0
    else:
        error_count = int(error_count[1])

    return (
        drive_stats,
        {
            'unscrubbed': unscrubbed_percent,
            'scrub_age': int(scrub_info[1]),
            'median': int(scrub_info[2]),
            'newest': int(scrub_info[3])
        },
        error_count,
        zero_subsecond_count
    )


def get_diff():
    snapraid_diff = run_snapraid(['diff'])

    diff_regex = re.compile(r'''^ +(?P<equal>\d+) equal$
^ +(?P<added>\d+) added$
^ +(?P<removed>\d+) removed$
^ +(?P<updated>\d+) updated$
^ +(?P<moved>\d+) moved$
^ +(?P<copied>\d+) copied$
^ +(?P<restored>\d+) restored$''', flags=re.MULTILINE)
    [diff_data] = [m.groupdict() for m in diff_regex.finditer(snapraid_diff)]

    if diff_data is None:
        msg = 'Unable to parse diff output from SnapRAID, not proceeding.'
        log.error(msg)
        notify_warning(msg)
        exit(1)

    diff_int = dict([a, int(x)] for a, x in diff_data.items())

    return diff_int


def get_smart():
    smart_data = run_snapraid(['smart'])

    drive_regex = re.compile(
        r'^ +(?P<temp>\d+|-) +(?P<power_on_days>\d+|-) +(?P<error_count>\d+|-) +(?P<fp>\d+%|-|SSD) +(?P<size>\S+) +('
        r'?P<serial>\S+) +(?P<device>\S+) +(?P<disk>\S+)$',
        flags=re.MULTILINE)
    drive_data = [m.groupdict() for m in drive_regex.finditer(smart_data)]

    global_fp = re.search(r'next year is (?P<total_fp>\d+)%', smart_data).group(1)

    if drive_data is None or global_fp is None:
        msg = 'Unable to parse drive data or global failure percentage, not proceeding.'
        log.error(msg)
        notify_warning(msg)
        exit(1)

    return (drive_data, global_fp)


def run_sync():
    start = datetime.now()
    sync_output = run_snapraid(['sync', '-h', '-q'] if config['prehash'] else ['sync', '-q'])
    end = datetime.now()

    check_completed_status(sync_output, 'SYNC')

    return end - start


def run_scrub():
    start = datetime.now()

    if config['scrub_new']:
        log.info('Scrubbing new blocks...')
        scrub_new_output = run_snapraid(['scrub', '-p', 'new', '-q'])

        check_completed_status(scrub_new_output, 'SCRUB NEW')

    log.info('Scrubbing old blocks...')
    scrub_output = run_snapraid(['scrub', '-p', str(config['scrub_percent']), '-o', str(config['scrub_age']), '-q'])

    end = datetime.now()

    check_completed_status(scrub_output, 'SCRUB')

    return end - start


def run_touch():
    run_snapraid(['touch'])


def check_completed_status(message, job_type):
    if not re.search(r'^Everything OK', message, flags=re.MULTILINE) and not re.search(r'^Nothing to do', message,
                                                                                       flags=re.MULTILINE):
        msg = f'SnapRAID {job_type} job did not finish as expected, please check your logs. Remaining jobs have been cancelled.'
        log.error(msg)
        notify_warning(msg)
        exit(1)

def sanity_check():
    config_file = config['snapraid_config_file']

    if not os.path.isfile(config_file):
        msg = f'Unable to find SnapRAID configuration file at "{config_file}", unable to proceed.'
        log.error(msg)
        notify_warning(msg)
        exit(1)

    with open(config_file, 'r') as f:
        config_content = f.read()

    file_regex = re.compile(r'^(?:content|parity) +(.+\/snapraid.(?:content|parity)) *$', flags=re.MULTILINE)
    files = [m[1] for m in file_regex.finditer(config_content)]

    for f in files:
        if not os.path.isfile(f):
            msg = f'Unable to locate required file "{f}", halting all execution.'
            notify_warning(msg)
            log.error(msg)
            exit(1)

    log.info(f'All {len(files)} content and parity files found, proceeding.')

#
# Main

def main():
    # Metadata for report
    sync_job_time = ''
    scrub_job_time = ''
    sync_job_ran = False
    scrub_job_ran = False

    total_start = datetime.now()

    log.info('Snapper started')

    log.info('Running sanity checks...')

    if not force_script_execution:
        sanity_check()

    log.info('Checking for errors and files with zero sub-second timestamps...')

    (_, _, error_count, zero_subsecond_count) = get_status()

    if error_count > 0 and not force_script_execution:
        msg = f'There are {error_count} errors in you array, you should review this immediately. All jobs have been halted.'
        log.error(msg)
        notify_warning(msg)
        exit(1)

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

    if sum(diff_data.values()) - diff_data["equal"] > 0:
        if force_script_execution:
            log.info('Ignoring added threshold and forcefully proceeding.')
        elif 0 < added_threshold < diff_data["added"]:
            msg = f'More files ({diff_data["added"]}) have been added than the configured max ({added_threshold}), not proceeding.'
            log.error(msg)
            notify_warning(msg)
            exit(0)
        else:
            log.info(
                f'Fewer files added ({diff_data["added"]}) than the configured limit ({added_threshold}), proceeding.')

        if force_script_execution:
            log.info('Ignoring added threshold and forcefully proceeding.')
        elif 0 < removed_threshold < diff_data["removed"]:
            msg = f'More files ({diff_data["removed"]}) have been removed than the configured max ({removed_threshold}), not proceeding.'
            log.error(msg)
            notify_warning(msg)
            exit(0)
        else:
            log.info(
                f'Fewer files removed ({diff_data["removed"]}) than the configured limit ({removed_threshold}), proceeding.')

        sync_job_ran = True
        log.info(f'Running SnapRAID sync {"with" if config["prehash"] else "without"} pre-hashing...')
        elapsed_time = run_sync()
        sync_job_time = format_delta(elapsed_time)
        log.info(f'Sync job finished, elapsed time {sync_job_time}')
    else:
        log.info('No changes to sync, skipping.')

    if config['scrub_percent'] > 0:
        scrub_job_ran = True
        log.info('Running scrub job...')
        elapsed_time = run_scrub()
        scrub_job_time = format_delta(elapsed_time)
        log.info(f'Scrub job finished, elapsed time {scrub_job_time}')
    else:
        log.info('Scrubbing not enabled, skipping.')

    log.info('Fetching SnapRAID status...')
    (drive_stats, scrub_stats, error_count, _) = get_status()

    log.info(
        f'{scrub_stats["unscrubbed"]}% of the array has not been scrubbed, with the oldest block at {scrub_stats["scrub_age"]} day(s), the median at {scrub_stats["median"]} day(s), and the newest at {scrub_stats["newest"]} day(s).')

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


main()
