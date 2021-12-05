USE [Call_DW]
GO

/****** Object:  StoredProcedure [Import].[usp_Import_Calls_Received_CallbackRequest]    Script Date: 9/16/2021 4:24:39 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF OBJECT_ID('Import.usp_Import_Calls_Received_CallbackRequest', 'P') IS NOT NULL
    DROP PROCEDURE Import.usp_Import_Calls_Received_CallbackRequest
GO

CREATE PROCEDURE [Import].[usp_Import_Calls_Received_CallbackRequest]
(
	@json NVARCHAR(MAX)
)
AS
-- =============================================
-- Author:		Bill Kea
-- Create date: 08/21/2021
-- Description:	Imports JSON documents into Import_CallsReceived_CallbackRequest table
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
		--  CALLBACK REQUEST
		SELECT
			IvrId,
			GeneralInfo_CallId,
			CallbackRequest_Id,
			CallbackRequest_CreationDate = TRY_PARSE(CallbackRequest_CreationDate AS DATETIME USING 'en-us'),
			CallbackRequest_QueueId,
			CallbackRequest_Priority,
			CallbackRequest_ExtraInfo,
			CallbackRequest_PopUrl,
			CallbackRequest_DestPhoneNumber,
			CallbackRequest_DestContactName,
			CallbackRequest_DefaultCallerType,
			CallbackRequest_DefaultMainSubject,
			CallbackRequest_DefaultSubsubject,
			CallbackRequest_IsAnswered,
			CallbackRequest_DeliveryInfo_StartDate = TRY_PARSE(CallbackRequest_DeliveryInfo_StartDate AS DATETIME USING 'en-us'),
			CallbackRequest_DeliveryInfo_EndDate = TRY_PARSE(CallbackRequest_DeliveryInfo_EndDate AS DATETIME USING 'en-us'),
			CallbackRequest_DeliveryInfo_RetryMinutesInterval,
			CallbackRequest_DeliveryInfo_LastDeliveryAttemptUtc = TRY_PARSE(CallbackRequest_DeliveryInfo_LastDeliveryAttemptUtc AS DATETIME USING 'en-us'),
			CallbackRequest_DeliveryInfo_DeliveryAttempts,
			CallbackRequest_DeliveryInfo_ExpirationMinutes,
			CallbackRequest_DeliveryInfo_MaxDeliveryAttempts
		INTO
			#CallsReceived_CallbackRequest
		FROM OPENJSON(@json)
			WITH (
			IvrId INT '$.IvrId',
			GeneralInfo_CallId VARCHAR(20) '$.GeneralInfo.CallId',
			CallbackRequest_Id VARCHAR(40) '$.CallbackRequest.Id',
			CallbackRequest_CreationDate VARCHAR(50) '$.CallbackRequest.CreationDate',
			CallbackRequest_QueueId INT '$.CallbackRequest.QueueId',
			CallbackRequest_Priority INT '$.CallbackRequest.Priority',
			CallbackRequest_ExtraInfo VARCHAR(20) '$.CallbackRequest.ExtraInfo',
			CallbackRequest_PopUrl VARCHAR(20) '$.CallbackRequest.PopUrl',
			CallbackRequest_DestPhoneNumber VARCHAR(20) '$.CallbackRequest.DestPhoneNumber',
			CallbackRequest_DestContactName VARCHAR(20) '$.CallbackRequest.DestContactName',
			CallbackRequest_DefaultCallerType VARCHAR(20) '$.CallbackRequest.DefaultCallerType',
			CallbackRequest_DefaultMainSubject VARCHAR(20) '$.CallbackRequest.DefaultMainSubject',
			CallbackRequest_DefaultSubsubject VARCHAR(20) '$.CallbackRequest.DefaultSubsubject',
			CallbackRequest_IsAnswered BIT '$.CallbackRequest.IsAnswered',
			CallbackRequest_DeliveryInfo_StartDate VARCHAR(50) '$.CallbackRequest.DeliveryInfo.StartDate',
			CallbackRequest_DeliveryInfo_EndDate VARCHAR(50) '$.CallbackRequest.DeliveryInfo.EndDate',
			CallbackRequest_DeliveryInfo_RetryMinutesInterval INT '$.CallbackRequest.DeliveryInfo.RetryMinutesInterval',
			CallbackRequest_DeliveryInfo_LastDeliveryAttemptUtc VARCHAR(50) '$.CallbackRequest.DeliveryInfo.LastDeliveryAttemptUtc',
			CallbackRequest_DeliveryInfo_DeliveryAttempts INT '$.CallbackRequest.DeliveryInfo.DeliveryAttempts',
			CallbackRequest_DeliveryInfo_ExpirationMinutes INT '$.CallbackRequest.DeliveryInfo.ExpirationMinutes',
			CallbackRequest_DeliveryInfo_MaxDeliveryAttempts INT '$.CallbackRequest.DeliveryInfo.MaxDeliveryAttempts'
			)
		WHERE 
			CallbackRequest_Id IS NOT NULL
		ORDER BY
			IvrId

		INSERT INTO [Import].[CallsReceived_CallbackRequest]
		(
			IvrId,
			GeneralInfo_CallId,
			CallbackRequest_Id,
			CallbackRequest_CreationDate,
			CallbackRequest_QueueId,
			CallbackRequest_Priority,
			CallbackRequest_ExtraInfo,
			CallbackRequest_PopUrl,
			CallbackRequest_DestPhoneNumber,
			CallbackRequest_DestContactName,
			CallbackRequest_DefaultCallerType,
			CallbackRequest_DefaultMainSubject,
			CallbackRequest_DefaultSubsubject,
			CallbackRequest_IsAnswered,
			CallbackRequest_DeliveryInfo_StartDate,
			CallbackRequest_DeliveryInfo_EndDate,
			CallbackRequest_DeliveryInfo_RetryMinutesInterval,
			CallbackRequest_DeliveryInfo_LastDeliveryAttemptUtc,
			CallbackRequest_DeliveryInfo_DeliveryAttempts,
			CallbackRequest_DeliveryInfo_ExpirationMinutes,
			CallbackRequest_DeliveryInfo_MaxDeliveryAttempts
		)
			SELECT
				IvrId,
				GeneralInfo_CallId,
				CallbackRequest_Id,
				CallbackRequest_CreationDate,
				CallbackRequest_QueueId,
				CallbackRequest_Priority,
				CallbackRequest_ExtraInfo,
				CallbackRequest_PopUrl,
				CallbackRequest_DestPhoneNumber,
				CallbackRequest_DestContactName,
				CallbackRequest_DefaultCallerType,
				CallbackRequest_DefaultMainSubject,
				CallbackRequest_DefaultSubsubject,
				CallbackRequest_IsAnswered,
				CallbackRequest_DeliveryInfo_StartDate,
				CallbackRequest_DeliveryInfo_EndDate,
				CallbackRequest_DeliveryInfo_RetryMinutesInterval,
				CallbackRequest_DeliveryInfo_LastDeliveryAttemptUtc,
				CallbackRequest_DeliveryInfo_DeliveryAttempts,
				CallbackRequest_DeliveryInfo_ExpirationMinutes,
				CallbackRequest_DeliveryInfo_MaxDeliveryAttempts
			FROM
				#CallsReceived_CallbackRequest

			EXCEPT
			SELECT
				IvrId,
				GeneralInfo_CallId,
				CallbackRequest_Id,
				CallbackRequest_CreationDate,
				CallbackRequest_QueueId,
				CallbackRequest_Priority,
				CallbackRequest_ExtraInfo,
				CallbackRequest_PopUrl,
				CallbackRequest_DestPhoneNumber,
				CallbackRequest_DestContactName,
				CallbackRequest_DefaultCallerType,
				CallbackRequest_DefaultMainSubject,
				CallbackRequest_DefaultSubsubject,
				CallbackRequest_IsAnswered,
				CallbackRequest_DeliveryInfo_StartDate,
				CallbackRequest_DeliveryInfo_EndDate,
				CallbackRequest_DeliveryInfo_RetryMinutesInterval,
				CallbackRequest_DeliveryInfo_LastDeliveryAttemptUtc,
				CallbackRequest_DeliveryInfo_DeliveryAttempts,
				CallbackRequest_DeliveryInfo_ExpirationMinutes,
				CallbackRequest_DeliveryInfo_MaxDeliveryAttempts

			FROM
				 [Import].[CallsReceived_CallbackRequest];	

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