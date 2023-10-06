from operator import itemgetter

did_not_run_color = 8539930
did_run_color = 1737287

empty_field = {'name': '** **', 'value': '** **'}


def create_discord_report(report_data):
    sync_job_ran, scrub_job_ran, sync_job_time, scrub_job_time, diff_data, zero_subsecond_count, \
        scrub_stats, drive_stats, smart_drive_data, global_fp, total_time = itemgetter(
            'sync_job_ran',
            'scrub_job_ran',
            'sync_job_time',
            'scrub_job_time',
            'diff_data',
            'zero_subsecond_count',
            'scrub_stats',
            'drive_stats',
            'smart_drive_data',
            'global_fp',
            'total_time')(report_data)

    touch_embed = {'title': 'Touch Job'}

    if zero_subsecond_count > 0:
        touch_embed['description'] = (f'A total of **{zero_subsecond_count}** file(s) had their '
                                      f'sub-second value fixed.')
        touch_embed['color'] = did_run_color
    else:
        touch_embed['description'] = 'No zero sub-second files were found.'
        touch_embed['color'] = did_not_run_color

    sync_embed = {'title': 'Sync Job'}

    if sync_job_ran:
        sync_embed['color'] = did_run_color
        sync_embed['description'] = 'Sync Job finished successfully :white_check_mark:'
        sync_embed['fields'] = [{
            'name': 'Added',
            'value': f'```{diff_data["added"]}```',
            'inline': True
        }, {
            'name': 'Removed',
            'value': f'```{diff_data["removed"]}```',
            'inline': True
        }, {
            'name': 'Updated',
            'value': f'```{diff_data["updated"]}```',
            'inline': True
        }, {
            'name': 'Moved',
            'value': f'```{diff_data["moved"]}```',
            'inline': True
        }, {
            'name': 'Copied',
            'value': f'```{diff_data["copied"]}```',
            'inline': True
        }, {
            'name': 'Restored',
            'value': f'```{diff_data["restored"]}```',
            'inline': True
        }]

        sync_embed['footer'] = {
            'text': f'Elapsed time {sync_job_time}'
        }
    else:
        sync_embed['color'] = did_not_run_color
        sync_embed['description'] = 'Sync job did **not** run.'

    scrub_embed = {'title': 'Scrub Job'}

    if scrub_job_ran:
        scrub_embed['color'] = did_run_color
        scrub_embed['description'] = f'''Scrub Job finished successfully :white_check_mark:
        
**{scrub_stats["unscrubbed"]}%** of the array has not been scrubbed, with the oldest block at **{scrub_stats["scrub_age"]}** day(s), the median at **{scrub_stats["median"]}** day(s), and the newest at **{scrub_stats["newest"]}** day(s).'''

        scrub_embed['footer'] = {
            'text': f'Elapsed time {scrub_job_time}'
        }
    else:
        scrub_embed['description'] = f'Scrub job did **not** run.'

    array_report_embed = {
        'title': 'Full Array Report',
        'color': did_run_color,
        'fields': []
    }

    for i, d in enumerate(drive_stats):
        field = {
            'name': d['drive_name'] if d['drive_name'] else 'Full Array',
            'value': f'''```
Total use (%)     {d["use_percent"]}
Fragmented Files  {d["fragmented_files"]}
Excess Fragments  {d["excess_fragments"]}
Wasted Space (GB) {d["wasted_gb"]}
Used Space (GB)   {d["used_gb"]}
Free Space (GB)   {d["free_gb"]}
```'''.replace(' ', '\u00A0'),
            'inline': True
        }

        array_report_embed['fields'].append(field)

        if (i + 1) % 2 == 0 and i + 1 != len(drive_stats):
            array_report_embed['fields'].append(empty_field)

    smart_report_embed = {
        'title': 'SMART Report',
        'description': f'The current failure probability of any single drive this year is {global_fp}%.',
        'color': did_run_color,
        'fields': [],
    }

    for i, d in enumerate(smart_drive_data):
        field = {
            'name': f'{d["device"]} (`{d["serial"]}`)' if d['disk'] == '-' else f'{d["disk"]} ({d["device"]}, `{d["serial"]}`)',
            'value': f'''```
Temperature (°C)     {d["temp"]}
Power On Time (days) {d["power_on_days"]}  
Error Count          {d["error_count"]}
Failure Probability  {d["fp"]}
Drive Size (TiB)     {d["size"]}
```'''.replace(' ', '\u00A0'),
            'inline': True
        }

        smart_report_embed['fields'].append(field)

        if (i + 1) % 2 == 0 and i + 1 != len(smart_drive_data):
            smart_report_embed['fields'].append(empty_field)

    embeds = [
        touch_embed,
        sync_embed,
        scrub_embed,
        array_report_embed,
        smart_report_embed
    ]

    return f':turtle: SnapRAID job completed successfully in **{total_time}**', embeds
