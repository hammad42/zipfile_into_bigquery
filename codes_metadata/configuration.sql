insert into `qa-mfrm-data.mfrm_config_logging_data.jobs_config_data`
(
SourceTable,
DestinationTable,
Mode,
ConfigName,
Active,
Query,
ScheduleInterval
)

Values
(
'gs://qa-mfrm-mulesoft-data-bucket/GCPInbound/SleepAmplitude/',
'mfrm_customer_and_social.amplitude_api_data',
'CDC',
'AmplitudeEventsExport',
'True',
'{"source_bucket_name":"qa-mfrm-mulesoft-data-bucket","destination_bucket_name":"qa-mfrm-data-processing"}',
'1'
)





