from pathlib import Path

def format_delta(delta):
    hours, remainder = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    return '{:02}h {:02}m {:02}s'.format(int(hours), int(minutes), int(seconds))


def get_relative_path(parent_path, file):
    return Path(parent_path).parent / file