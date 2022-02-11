CREATE OR REPLACE PROCEDURE `dev-mfrm-data.mfrm_customer_and_social.sp_load_amplitude_api_data`()
BEGIN
-- =============================================
-- Last Updated By:		  Hammad Shamim
-- Description:           load amplitude events data into bq

  DECLARE StartDate TIMESTAMP;
  DECLARE rowCount int64;
  DECLARE mode string;
  DECLARE last_execution TIMESTAMP;
  
  SET mode = (SELECT mode FROM mfrm_config_logging_data.jobs_config_data where ConfigName ='AmplitudeEventsExport');
  SET rowCount = (Select count(*) from mfrm_staging_dataset.amplitude_api_data_stg);
  SET StartDate = CURRENT_TIMESTAMP();
  
  
  IF rowCount > 0 and mode = 'CDC'
    THEN
    
      DELETE from mfrm_customer_and_social.amplitude_api_data
      where trim(EventID) || trim(CAST(UNIX_MILLIS(CAST(UTCClientEventTime AS TIMESTAMP)) AS STRING)) || trim(SessionID) || trim(UUID)
      in (select trim(JSON_EXTRACT(JSONObject, "$.event_id")) || trim(JSON_EXTRACT(JSONObject, "$.client_event_time")) ||
      trim(JSON_EXTRACT(JSONObject, "$.session_id")) || replace(trim(JSON_EXTRACT(JSONObject, "$.uuid")),'"','')
      from mfrm_staging_dataset.amplitude_api_data_stg );
    
      INSERT INTO mfrm_customer_and_social.amplitude_api_data
      (
      App	,
      DeviceID	,
      UserID	,
      UTCClientEventTime	,
      CSTClientEventTime	,
      EventID	,
      SessionID	,
      EventType	,
      AmplitudeEventType	,
      VersionName	,
      Platform	,
      OSName	,
      OSVersion	,
      DeviceBrand	,
      DeviceManufacturer	,
      DeviceModel	,
      DeviceFamily	,
      DeviceType	,
      DeviceCarrier	,
      LocationLat	,
      LocationLng	,
      IPAddress	,
      Country	,
      Language	,
      Library	,
      City	,
      Region	,
      DMA	,
      EventProperties,
      UserProperties,
      GlobalUserProperties	,
      GroupProperties	,
      UTCEventTime	,
      CSTEventTime	,
      UTCClientUploadTime	,
      CSTClientUploadTime	,
      UTCServerUploadTime	,
      CSTServerUploadTime	,
      UTCServerReceivedTime	,
      CSTServerReceivedTime	,
      AmplitudeID	,
      IDFA	,
      ADID	,
      Data	,
      Paying	,
      StartVersion	,
      CSTUserCreationTime	,
      UTCUserCreationTime	,
      UUID	,
      Groupss	,
      SampleRate	,
      InsertID	,
      InsertKey	,
      IsAttributionEvent	,
      AmplitudeAttributionIDs	,
      Plan	,
      PartnerID	,
      Schema	,
      CSTProcessedTime	,
      UTCProcessedTime	,
      CreatedBy	,
      ModifiedBy	,
      CreatedDateTime	,
      ModifiedDateTime	
      )
      select
      replace(JSON_EXTRACT(JSONObject,"$.app"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.device_id"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.user_id"),'"',''),
      EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(cast(JSON_EXTRACT(JSONObject,"$.client_event_time")as integer))),
      DateTime(Cast(EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(cast(JSON_EXTRACT(JSONObject,"$.client_event_time")as integer)))as Timestamp),"America/Chicago"),
      replace(JSON_EXTRACT(JSONObject,"$.event_id"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.session_id"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_type"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.amplitude_event_type"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.version_name"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.platform"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.os_name"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.os_version"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.device_brand"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.device_manufacturer"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.device_model"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.device_family"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.device_type"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.device_carrier"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.location_lat"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.location_lng"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.ip_address"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.country"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.language"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.library"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.city"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.region"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.dma"),'"',''),
      (replace(JSON_EXTRACT(JSONObject,"$.event_properties.Activity"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.BedtimeScreen"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.BenefitsScreen"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.Count"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.Destination"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.Duration"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.EmailSignUpScreen"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.Flow"),'"',''),
      datetime(TIMESTAMP(CASE
              WHEN JSON_EXTRACT(JSONObject, "$.event_properties.HistoryStartDate") LIKE '%PM%' THEN STRING(TIMESTAMP_ADD(TIMESTAMP(REPLACE(REPLACE(REPLACE(JSON_EXTRACT(JSONObject, "$.event_properties.HistoryStartDate"),' +','+'),'"',''),' PM','')),INTERVAL 12 HOUR))
              WHEN JSON_EXTRACT(JSONObject,
              "$.event_properties.HistoryStartDate") LIKE '%AM%' THEN REPLACE(REPLACE(REPLACE(JSON_EXTRACT(JSONObject,
                    "$.event_properties.HistoryStartDate"),' +','+'),'"',''),' AM','')
              WHEN JSON_EXTRACT(JSONObject,
              "$.event_properties.HistoryStartDate") LIKE '%am%' THEN REPLACE(REPLACE(REPLACE(JSON_EXTRACT(JSONObject,
                    "$.event_properties.HistoryStartDate"),' +','+'),'"',''),' am','')
              WHEN JSON_EXTRACT(JSONObject,
              "$.event_properties.HistoryStartDate") LIKE '%pm%' THEN REPLACE(REPLACE(REPLACE(JSON_EXTRACT(JSONObject,
                    "$.event_properties.HistoryStartDate"),' +','+'),'"',''),' pm','')
            ELSE
            REPLACE(REPLACE(JSON_EXTRACT(JSONObject,
                  "$.event_properties.HistoryStartDate"),'"',''),' +','+')
          END
            ) ),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.HourOfDay"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.IsDstNotification"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.LocalTime"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.MessageId"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.NotificationDeliveryDate"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.NuggetId"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.NuggetTitle"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.Parent"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.PlugInPhoneModal"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.PluginPhoneModel"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.PollId"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.Polls_Answer"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.ProfileScreen"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.Reason"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.Result"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.SbuscribeNowModal"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.State"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.SubscribeNowModal"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.SubscriptionScreen"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.TrackingScreen"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.Type"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.depth"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.healthKitSet"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.offset"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.source"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.source:"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.userPreviouslySet"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.userSet"),'"','')),
      (replace(JSON_EXTRACT(JSONObject,"$.user_properties.Experiment_SleepCoachCopy"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.user_properties.HashedUserId"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.user_properties.isPremium"),'"','')),
      replace(JSON_EXTRACT(JSONObject,"$.global_user_properties"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.group_properties"),'"',''),
      EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(cast(JSON_EXTRACT(JSONObject,"$.event_time")as integer))),
      DateTime(Cast(EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(cast(JSON_EXTRACT(JSONObject,"$.event_time")as integer)))as Timestamp),"America/Chicago"),
      EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(cast(JSON_EXTRACT(JSONObject,"$.client_upload_time")as integer))),
      DateTime(Cast(EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(cast(JSON_EXTRACT(JSONObject,"$.client_upload_time")as integer)))as Timestamp),"America/Chicago"),
      EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(cast(JSON_EXTRACT(JSONObject,"$.server_upload_time")as integer))),
      DateTime(Cast(EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(cast(JSON_EXTRACT(JSONObject,"$.server_upload_time")as integer)))as Timestamp),"America/Chicago"),
      EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(cast(JSON_EXTRACT(JSONObject,"$.server_received_time")as integer))),
      DateTime(Cast(EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(cast(JSON_EXTRACT(JSONObject,"$.server_received_time")as integer)))as Timestamp),"America/Chicago"),
      replace(JSON_EXTRACT(JSONObject,"$.amplitude_id"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.idfa"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.adid"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.data"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.paying"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.start_version"),'"',''),
      DateTime(Cast(EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(cast(JSON_EXTRACT(JSONObject,"$.user_creation_time")as integer)))as Timestamp),"America/Chicago"),
      EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(cast(JSON_EXTRACT(JSONObject,"$.user_creation_time")as integer))),
      replace(JSON_EXTRACT(JSONObject,"$.uuid"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.groups"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.sample_rate"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.insert_id"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.insert_key"),'"',''),
      cast(JSON_EXTRACT(JSONObject,"$.is_attribution_event")as boolean),
      replace(replace(replace(JSON_EXTRACT(JSONObject,"$.amplitude_attribution_ids"),'"',''),'[',''),']',''),
      replace(JSON_EXTRACT(JSONObject,"$.plan"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.partner_id"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.schema"),'"',''),
      DateTime(Cast(EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(cast(JSON_EXTRACT(JSONObject,"$.processed_time")as integer)))as Timestamp),"America/Chicago"),
      EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(cast(JSON_EXTRACT(JSONObject,"$.processed_time")as integer))),
      'load_amplitude_event',
      'load_amplitude_event',
      CURRENT_DATETIME('America/Chicago'),
      CURRENT_DATETIME('America/Chicago')

      from (SELECT
      replace( JSONObject,'$','') JSONObject
      FROM mfrm_staging_dataset.amplitude_api_data_stg
      where jsonobject like '%insert_key%'
      );
    
        Insert into  `mfrm_config_logging_data.logging_data` ( 
        FunctionName, 
        ProcessedRows, 
        FailedRows, 
        StartDateTime, 
        EndDateTime, 
        Status, 
        TargetTable, 
        Message, 
        JobType, 
        Source )  
      values (
        'load_amplitude_api_data_to_bq', cast(rowCount as Numeric),0, 
        CAST(DATETIME(StartDate,"America/Chicago") AS STRING) ,CAST(current_datetime("America/Chicago") AS STRING),
       'Success','mfrm_customer_and_social.amplitude_api_data','amplitude events CDC Load Process Completed','Delete/Insert','Amplitude API');
       
    SET last_execution = (select timestamp(max(CSTClientEventTime)) FROM mfrm_customer_and_social.amplitude_api_data);
    
       -- updating config entry	
    update `mfrm_config_logging_data.jobs_config_data` 
        set 
        Mode='CDC',
        startdate = StartDate, 
        EndDate = CURRENT_TIMESTAMP(), 
        LastExecutionTime = last_execution
      where ConfigName like 'AmplitudeEventsExport';
    
    
  END IF;
  
  IF rowCount > 0 and mode = 'FullLoad'
    THEN
      TRUNCATE TABLE mfrm_customer_and_social.amplitude_api_data;
      INSERT INTO mfrm_customer_and_social.amplitude_api_data
        (
        App	,
        DeviceID	,
        UserID	,
        UTCClientEventTime	,
        CSTClientEventTime	,
        EventID	,
        SessionID	,
        EventType	,
        AmplitudeEventType	,
        VersionName	,
        Platform	,
        OSName	,
        OSVersion	,
        DeviceBrand	,
        DeviceManufacturer	,
        DeviceModel	,
        DeviceFamily	,
        DeviceType	,
        DeviceCarrier	,
        LocationLat	,
        LocationLng	,
        IPAddress	,
        Country	,
        Language	,
        Library	,
        City	,
        Region	,
        DMA	,
        EventProperties,
        UserProperties,
        GlobalUserProperties	,
        GroupProperties	,
        UTCEventTime	,
        CSTEventTime	,
        UTCClientUploadTime	,
        CSTClientUploadTime	,
        UTCServerUploadTime	,
        CSTServerUploadTime	,
        UTCServerReceivedTime	,
        CSTServerReceivedTime	,
        AmplitudeID	,
        IDFA	,
        ADID	,
        Data	,
        Paying	,
        StartVersion	,
        CSTUserCreationTime	,
        UTCUserCreationTime	,
        UUID	,
        Groupss	,
        SampleRate	,
        InsertID	,
        InsertKey	,
        IsAttributionEvent	,
        AmplitudeAttributionIDs	,
        Plan	,
        PartnerID	,
        Schema	,
        CSTProcessedTime	,
        UTCProcessedTime	,
        CreatedBy	,
        ModifiedBy	,
        CreatedDateTime	,
        ModifiedDateTime	
        )
        select
      replace(JSON_EXTRACT(JSONObject,"$.app"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.device_id"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.user_id"),'"',''),
      EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(cast(JSON_EXTRACT(JSONObject,"$.client_event_time")as integer))),
      DateTime(Cast(EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(cast(JSON_EXTRACT(JSONObject,"$.client_event_time")as integer)))as Timestamp),"America/Chicago"),
      replace(JSON_EXTRACT(JSONObject,"$.event_id"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.session_id"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_type"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.amplitude_event_type"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.version_name"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.platform"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.os_name"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.os_version"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.device_brand"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.device_manufacturer"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.device_model"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.device_family"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.device_type"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.device_carrier"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.location_lat"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.location_lng"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.ip_address"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.country"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.language"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.library"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.city"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.region"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.dma"),'"',''),
      (replace(JSON_EXTRACT(JSONObject,"$.event_properties.Activity"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.BedtimeScreen"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.BenefitsScreen"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.Count"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.Destination"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.Duration"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.EmailSignUpScreen"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.Flow"),'"',''),
      datetime(TIMESTAMP(CASE
              WHEN JSON_EXTRACT(JSONObject, "$.event_properties.HistoryStartDate") LIKE '%PM%' THEN STRING(TIMESTAMP_ADD(TIMESTAMP(REPLACE(REPLACE(REPLACE(JSON_EXTRACT(JSONObject, "$.event_properties.HistoryStartDate"),' +','+'),'"',''),' PM','')),INTERVAL 12 HOUR))
              WHEN JSON_EXTRACT(JSONObject,
              "$.event_properties.HistoryStartDate") LIKE '%AM%' THEN REPLACE(REPLACE(REPLACE(JSON_EXTRACT(JSONObject,
                    "$.event_properties.HistoryStartDate"),' +','+'),'"',''),' AM','')
            ELSE
            REPLACE(REPLACE(JSON_EXTRACT(JSONObject,
                  "$.event_properties.HistoryStartDate"),'"',''),' +','+')
          END
            ) ),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.HourOfDay"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.IsDstNotification"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.LocalTime"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.MessageId"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.NotificationDeliveryDate"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.NuggetId"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.NuggetTitle"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.Parent"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.PlugInPhoneModal"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.PluginPhoneModel"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.PollId"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.Polls_Answer"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.ProfileScreen"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.Reason"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.Result"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.SbuscribeNowModal"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.State"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.SubscribeNowModal"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.SubscriptionScreen"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.TrackingScreen"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.Type"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.depth"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.healthKitSet"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.offset"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.source"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.source:"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.userPreviouslySet"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.event_properties.userSet"),'"','')),
      (replace(JSON_EXTRACT(JSONObject,"$.user_properties.Experiment_SleepCoachCopy"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.user_properties.HashedUserId"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.user_properties.isPremium"),'"','')),
      replace(JSON_EXTRACT(JSONObject,"$.global_user_properties"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.group_properties"),'"',''),
      EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(cast(JSON_EXTRACT(JSONObject,"$.event_time")as integer))),
      DateTime(Cast(EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(cast(JSON_EXTRACT(JSONObject,"$.event_time")as integer)))as Timestamp),"America/Chicago"),
      EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(cast(JSON_EXTRACT(JSONObject,"$.client_upload_time")as integer))),
      DateTime(Cast(EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(cast(JSON_EXTRACT(JSONObject,"$.client_upload_time")as integer)))as Timestamp),"America/Chicago"),
      EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(cast(JSON_EXTRACT(JSONObject,"$.server_upload_time")as integer))),
      DateTime(Cast(EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(cast(JSON_EXTRACT(JSONObject,"$.server_upload_time")as integer)))as Timestamp),"America/Chicago"),
      EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(cast(JSON_EXTRACT(JSONObject,"$.server_received_time")as integer))),
      DateTime(Cast(EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(cast(JSON_EXTRACT(JSONObject,"$.server_received_time")as integer)))as Timestamp),"America/Chicago"),
      replace(JSON_EXTRACT(JSONObject,"$.amplitude_id"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.idfa"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.adid"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.data"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.paying"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.start_version"),'"',''),
      DateTime(Cast(EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(cast(JSON_EXTRACT(JSONObject,"$.user_creation_time")as integer)))as Timestamp),"America/Chicago"),
      EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(cast(JSON_EXTRACT(JSONObject,"$.user_creation_time")as integer))),
      replace(JSON_EXTRACT(JSONObject,"$.uuid"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.groups"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.sample_rate"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.insert_id"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.insert_key"),'"',''),
      cast(JSON_EXTRACT(JSONObject,"$.is_attribution_event")as boolean),
      replace(replace(replace(JSON_EXTRACT(JSONObject,"$.amplitude_attribution_ids"),'"',''),'[',''),']',''),
      replace(JSON_EXTRACT(JSONObject,"$.plan"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.partner_id"),'"',''),
      replace(JSON_EXTRACT(JSONObject,"$.schema"),'"',''),
      DateTime(Cast(EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(cast(JSON_EXTRACT(JSONObject,"$.processed_time")as integer)))as Timestamp),"America/Chicago"),
      EXTRACT(DATETIME FROM TIMESTAMP_MILLIS(cast(JSON_EXTRACT(JSONObject,"$.processed_time")as integer))),
      'load_amplitude_event',
      'load_amplitude_event',
      CURRENT_DATETIME('America/Chicago'),
      CURRENT_DATETIME('America/Chicago')
    

    from (SELECT
      replace( JSONObject,'$','') JSONObject
      FROM mfrm_staging_dataset.amplitude_api_data_stg
      where jsonobject like '%insert_key%'
      );
    
    Insert into  `mfrm_config_logging_data.logging_data` ( 
        FunctionName, 
        ProcessedRows, 
        FailedRows, 
        StartDateTime, 
        EndDateTime, 
        Status, 
        TargetTable, 
        Message, 
        JobType, 
        Source )  
      values (
        'load_amplitude_api_data_to_bq', cast(rowCount as Numeric),0, 
        CAST(DATETIME(StartDate,"America/Chicago") AS STRING) ,CAST(current_datetime("America/Chicago") AS STRING),
       'Success','mfrm_customer_and_social.amplitude_api_data','amplitude api Full Load Process Completed','Truncate/Load','Amplitude API');
       
      SET last_execution = (select timestamp(max(CSTClientEventTime)) FROM mfrm_customer_and_social.amplitude_api_data);
       
       -- updating config entry	
    update `mfrm_config_logging_data.jobs_config_data` 
        set 
        Mode='CDC',
        startdate = StartDate, 
        EndDate = CURRENT_TIMESTAMP(), 
        LastExecutionTime = last_execution
      where ConfigName like 'AmplitudeEventsExport';
    
  END IF;
  
END;