USE [Call_DW]
GO

/****** Object:  StoredProcedure [Import].[usp_Import_Calls_Received_VoiceMail]    Script Date: 9/16/2021 4:24:39 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF OBJECT_ID('Import.usp_Import_Calls_Received_VoiceMail', 'P') IS NOT NULL
    DROP PROCEDURE Import.usp_Import_Calls_Received_VoiceMail
GO

CREATE PROCEDURE [Import].[usp_Import_Calls_Received_VoiceMail]
(
	@json NVARCHAR(MAX)
)
AS
-- =============================================
-- Author:		Bill Kea
-- Create date: 08/21/2021
-- Description:	Imports JSON documents into Import_Calls_Received_VoiceMail table
-- =============================================
BEGIN
	/*
	  DEFINE LOCAL VARIABLES
	*/
	DECLARE @error_number AS INT
	DECLARE @error_state AS INT
	DECLARE @error_severity AS INT
	DECLARE @error_procedure AS NVARCHAR(120)
	DECLARE @error_line AS INT
	DECLARE @error_message NVARCHAR(4000) 

	DECLARE @error_cnt AS INT = 0;
	DECLARE @error_max AS INT = 3;

	 /*
	   BUILD OUT ERROR TABLE
	 */
	SELECT * INTO #DeadLockErrors FROM master.dbo.sysmessages WHERE description LIKE '%DEADLOCK%' AND msglangid = 1033;


	RETRYINSERT:
	BEGIN TRY
		IF (ISJSON(@json) = 0) 	
		BEGIN
			RAISERROR ('Invalid or empty Calls Received JSON document',11,1) WITH NOWAIT;
		END

		BEGIN TRAN

		--  VOICE MAIL
		SELECT
			IvrId,
			GeneralInfo_CallId,
			VoiceMail_ID,
			VoiceMail_ReferenceNo,
			VoiceMail_SecondsDuration,
			VoiceMail_EntryQueueId,
			VoiceMail_CreationDate = TRY_PARSE(VoiceMail_CreationDate AS DATETIME USING 'en-us')
		INTO
			#CallReceived_Voicemail
		FROM OPENJSON(@json)
			WITH (
			IvrId INT '$.IvrId',
			GeneralInfo_CallId VARCHAR(20) '$.GeneralInfo.CallId',
			VoiceMail_ID VARCHAR(20) '$.Voicemail.Id',
			VoiceMail_ReferenceNo VARCHAR(20) '$.Voicemail.ReferenceNo',
			VoiceMail_SecondsDuration VARCHAR(20) '$.Voicemail.SecondsDuration',
			VoiceMail_EntryQueueId VARCHAR(20) '$.Voicemail.EntryQueueId',
			VoiceMail_CreationDate VARCHAR(50) '$.Voicemail.CreationDate'
			)
		WHERE 
			VoiceMail_ID IS NOT NULL
		ORDER BY
			IvrId



		INSERT INTO [Import].[CallsReceived_VoiceMail]
		(
			IvrId,
			GeneralInfo_CallId,
			VoiceMail_ID,
			VoiceMail_ReferenceNo,
			VoiceMail_SecondsDuration,
			VoiceMail_EntryQueueId,
			VoiceMail_CreationDate
		)
			SELECT
				IvrId,
				GeneralInfo_CallId,
				VoiceMail_ID,
				VoiceMail_ReferenceNo,
				VoiceMail_SecondsDuration,
				VoiceMail_EntryQueueId,
				VoiceMail_CreationDate
			FROM 
				#CallReceived_Voicemail
			EXCEPT
			SELECT
				IvrId,
				GeneralInfo_CallId,
				VoiceMail_ID,
				VoiceMail_ReferenceNo,
				VoiceMail_SecondsDuration,
				VoiceMail_EntryQueueId,
				VoiceMail_CreationDate
			FROM
				 [Import].[CallsReceived_Voicemail];	

		COMMIT TRAN

	END TRY
	BEGIN CATCH
		ROLLBACK TRAN
		
	--  Determine if this is a deadlock issue
		IF EXISTS(SELECT error FROM #DeadLockErrors WHERE error = ERROR_NUMBER()) AND (@error_cnt < @error_max)
		BEGIN
			SET @error_cnt += 1;		--increment error loop counter
			WAITFOR DELAY '00:00:05';	--wait 5 milliseconds
			GOTO RETRYINSERT;			--retry transaction
		END
	
	--  Log Error into error table
		--INSERT INTO dbo.DB_Errors(err_number,err_state,err_severity,err_procedure,err_line,err_message,err_dt)
		--VALUES(ERROR_NUMBER(),ERROR_STATE(),ERROR_SEVERITY(),ERROR_PROCEDURE(),ERROR_LINE(),ERROR_MESSAGE(),GETDATE());

		SELECT 
			 @error_message = ERROR_MESSAGE()
			,@error_severity = ERROR_SEVERITY()
			,@error_state = ERROR_STATE()

		RAISERROR (@error_message,@error_severity,@error_state) WITH NOWAIT;  
	END CATCH

END
GO