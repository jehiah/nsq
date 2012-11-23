## nsqjobtracker

The job tracker manages metadata for running jobs on archived nsqd data. It coordinates job id's and notifies jobs of completion.


* /new_job - starts a new job
        name=....,
        topic=....,
        workers=....
        -> create job meta channel
        -> create channels on source nsqd's
        -> returns job id
 * /stop_job - pushes messages into the job channel

 poll nsqds; on source topic completion, push message through metadata channel
