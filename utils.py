import subprocess
from pathlib import Path

def format_delta(delta):
    hours, remainder = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    return '{:02}h {:02}m {:02}s'.format(int(hours), int(minutes), int(seconds))


def get_relative_path(parent_path, file):
    return Path(parent_path).parent / file


def human_readable_size(size):
    suffixes = ['MB', 'GB', 'TB', 'PB']
    i = 0
    while size >= 1024 and i < len(suffixes) - 1:
        size /= 1024.
        i += 1
    f = ('%.2f' % size).rstrip('0').rstrip('.')
    return '%s %s' % (f, suffixes[i])


def run_script(script_file):
    p = subprocess.Popen(script_file, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    retval = p.wait()

    if retval != 0:
        raise ChildProcessError(f'External script "{script_file}" returned status code {retval}.')
