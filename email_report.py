from utils import get_relative_path

with open(get_relative_path(__file__, './email_format.html'), 'r') as f:
    email_report_template = f.read()


def create_email_report(
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
):
    #
    # Create email report

    sync_report = f'<h3>Sync Job</h3>'

    if sync_job_ran:
        sync_report = sync_report + f'''
        <p>Job finished successfully in <strong>{sync_job_time}</strong>.</p>
        <p>File diff summary as follows:</p>
        <ul>
          <li>{diff_data["added"]} added</li>
          <li>{diff_data["removed"]} removed</li>
          <li>{diff_data["updated"]} updated</li>
          <li>{diff_data["moved"]} moved</li>
          <li>{diff_data["copied"]} copied</li>
          <li>{diff_data["restored"]} restored</li>
        </ul>
        '''
    else:
        sync_report = sync_report + '<p>Sync Job did <strong>not</strong> run.</p>'

    touch_report = '<h3>Touch job</h3>'

    if zero_subsecond_count > 0:
        touch_report = touch_report + '<p>A total of <strong>{zero_subsecond_count}</strong> file(s) had their ' \
                                      'sub-second value fixed.</p>'
    else:
        touch_report = touch_report + '<p>No zero sub-second files were found.</p>'

    scrub_report = '<h3>Scrub Job</h3>'

    if scrub_job_ran:
        scrub_report = scrub_report + f'''
        <p>Job finished successfully in <strong>{scrub_job_time}</strong>.</p>
        <p><strong>{scrub_stats["unscrubbed"]}%</strong> of the array has not been scrubbed, with the oldest block at 
        <strong>{scrub_stats["scrub_age"]}</strong> day(s), the median at <strong>{scrub_stats["median"]}</strong> 
        day(s), and the newest at <strong>{scrub_stats["newest"]}</strong> day(s).</p>
        '''
    else:
        scrub_report = scrub_report + '<p>Scrub Job did <strong>not</strong> run.</p>'

    array_drive_report = ''.join(f'''
    <tr class="{"array_stats" if not d["drive_name"] else ''}">
        <td>{d["drive_name"] if d["drive_name"] else 'Full Array'}</td>
        <td>{d["fragmented_files"]}</td>
        <td>{d["excess_fragments"]}</td>
        <td>{d["wasted_gb"]}</td>
        <td>{d["used_gb"]}</td>
        <td>{d["free_gb"]}</td>
        <td>{d["use_percent"]}</td>
    </tr>
    ''' for d in drive_stats)

    array_report = f'''
    <h3>SnapRAID Array Report</h3>
    <table>
        <thead>
            <tr>
                <th>Drive</th>
                <th>Fragmented Files</th>
                <th>Excess Fragments</th>
                <th>Wasted Space (GB)</th>
                <th>Used Space (GB)</th>
                <th>Free Space (GB)</th>
                <th>Total Used (%)</th>
            </tr>
        </thead>
        <tbody>
            {array_drive_report}
        </tbody>
    </table>
    '''

    smart_drive_report = ''.join(f'''
    <tr>
        <td>{d["disk"]} ({d["device"]})</td>
        <td>{d["temp"]}</td>
        <td>{d["power_on_days"]}</td>
        <td>{d["error_count"]}</td>
        <td>{d["fp"]}</td>
        <td>{d["size"]}</td>
        <td>{d["serial"]}</td>
    </tr>
    ''' for d in smart_drive_data)

    smart_report = f'''
    <h3>SMART Report</h3>
    <table>
        <thead>
            <tr>
                <th>Drive</th>
                <th>Temperature (°C)</th>
                <th>Power On Time (days)</th>
                <th>Error Count</th>
                <th>Failure Probability</th>
                <th>Drive Size (TiB)</th>
                <th>Serial Number</th>
            </tr>
        </thead>
        <tbody>
            {smart_drive_report}
        </tbody>
    </table>
    <p>The current failure probability of any single drive this year is <strong>{global_fp}%</strong>.</p>
    '''

    email_report = f'''
    <h2>[Snapper] SnapRAID job completed successfully in {total_time}</h2>
    {touch_report} 
    {sync_report}
    {scrub_report}
    {array_report}
    {smart_report}
    '''

    email_message = email_report_template.replace('SNAPRAID_REPORT_CONTENT', email_report)

    return email_message
