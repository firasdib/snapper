def format_delta(delta):
    hours, remainder = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    return '{:02}h {:02}m {:02}s'.format(int(hours), int(minutes), int(seconds))
