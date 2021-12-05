USE [Call_DW]
GO

/****** Object:  StoredProcedure [Import].[usp_Import_Calls_Received]    Script Date: 9/10/2021 1:59:08 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF OBJECT_ID('Import.usp_Import_Calls_Received', 'P') IS NOT NULL
    DROP PROCEDURE Import.usp_Import_Calls_Received
GO

CREATE PROCEDURE [Import].[usp_Import_Calls_Received]
(
	@json NVARCHAR(MAX)
)
AS
-- =============================================
-- Author:		Bill Kea
-- Create date: 08/21/2021
-- Description:	Imports JSON documents into Import.AgantSession table
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
			RAISERROR ('Invalid or empty Agent Session JSON document',11,1) WITH NOWAIT;
		END

		BEGIN TRAN
			EXEC [Import].[usp_Import_Calls_Received_AnsweredHistory] @json
			EXEC [Import].[usp_Import_Calls_Received_CallbackRequest] @json
			EXEC [Import].[usp_Import_Calls_Received_Checkpoints] @json
			EXEC [Import].[usp_Import_Calls_Received_Disposition] @json
			EXEC [Import].[usp_Import_Calls_Received_GeneralInfo] @json
			EXEC [Import].[usp_Import_Calls_Received_QueuingHistory] @json
			EXEC [Import].[usp_Import_Calls_Received_SipHeaders] @json
			EXEC [Import].[usp_Import_Calls_Received_TransferAttemptsHistory] @json
			EXEC [Import].[usp_Import_Calls_Received_VoiceMail] @json
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



		--	SELECT
		--		IvrId,
		--		VoiceMail_ID,
		--		VoiceMail_ReferenceNo,
		--		VoiceMail_SecondsDuration,
		--		VoiceMail_EntryQueueId,
		--		VoiceMail_CreationDate,
		--		CallbackRequest_Id,
		--		CallbackRequest_CreationDate,
		--		CallbackRequest_QueueId,
		--		CallbackRequest_Priority,
		--		CallbackRequest_ExtraInfo,
		--		CallbackRequest_PopUrl,
		--		CallbackRequest_DestPhoneNumber,
		--		CallbackRequest_DestContactName,
		--		CallbackRequest_DefaultCallerType,
		--		CallbackRequest_DefaultMainSubject,
		--		CallbackRequest_DefaultSubsubject,
		--		CallbackRequest_IsAnswered,
		--		CallbackRequest_DeliveryInfo_StartDate,
		--		CallbackRequest_DeliveryInfo_EndDate,
		--		CallbackRequest_DeliveryInfo_RetryMinutesInterval,
		--		CallbackRequest_DeliveryInfo_LastDeliveryAttemptUtc,
		--		CallbackRequest_DeliveryInfo_DeliveryAttempts,
		--		CallbackRequest_DeliveryInfo_ExpirationMinutes,
		--		CallbackRequest_DeliveryInfo_MaxDeliveryAttempts,
		--		SipHeaders_HeaderName,
		--		SipHeaders_HeaderValue,
		--		QueuingHistory_QueuingDate,
		--		QueuingHistory_QueueId,
		--		QueuingHistory_ExitQueueDate,
		--		QueuingHistory_ExitCode,
		--		AnsweredHistory_AnsweredDate,
		--		AnsweredHistory_AgentId,
		--		AnsweredHistory_AgentPhoneNumber,
		--		AnsweredHistory_WrapupStart,
		--		AnsweredHistory_WrapupEnd,
		--		AnsweredHistory_ACWStart,
		--		AnsweredHistory_ACWEnd,
		--		AnsweredHistory_HoldCount,
		--		AnsweredHistory_SecondsOnHold,
		--		Checkpoints_Id,
		--		Checkpoints_CreationDate,
		--		Checkpoints_Data,
		--		TransferAttemptsHistory_AgentSessionId,
		--		TransferAttemptsHistory_StartDate,
		--		TransferAttemptsHistory_PhoneNumber,
		--		TransferAttemptsHistory_CallTransferMode,
		--		TransferAttemptsHistory_EndDate,
		--		TransferAttemptsHistory_ExitCode,
		--		TransferAttemptsHistory_DstQueueId,
		--		GeneralInfo_ThreadId,
		--		GeneralInfo_CallId,
		--		GeneralInfo_StartDate,
		--		GeneralInfo_EndDate,
		--		GeneralInfo_OrgNumber,
		--		GeneralInfo_OrgName,
		--		GeneralInfo_DstNumber,
		--		GeneralInfo_DstName,
		--		GeneralInfo_ExitCode,
		--		GeneralInfo_OriginationReason,
		--		GeneralInfo_HangupEvtRd,
		--		GeneralInfo_Label,
		--		GeneralInfo_Direction,
		--		Disposition_ClientType,
		--		Disposition_ClientTypeRef1,
		--		Disposition_ClientTypeRef2,
		--		Disposition_ClientTypeRef3,
		--		Disposition_MainSubject,
		--		Disposition_Subsubject,
		--		Disposition_SubsubjectDetails,
		--		Disposition_Resolution,
		--		Disposition_IsFlagged,
		--		Disposition_FlagingReason,
		--		Disposition_Notes,
		--		Disposition_CreationDate,
		--		Disposition_CreatedBy
		--	INTO
		--		#temp_CallReceived
		--	FROM OPENJSON(@json)
		--	  WITH (
		--		IvrId INT '$.IvrId',
		--		VoiceMail_ID VARCHAR(20) '$.Voicemail.Id',
		--		VoiceMail_ReferenceNo VARCHAR(20) '$.Voicemail.ReferenceNo',
		--		VoiceMail_SecondsDuration VARCHAR(20) '$.Voicemail.SecondsDuration',
		--		VoiceMail_EntryQueueId VARCHAR(20) '$.Voicemail.EntryQueueId',
		--		VoiceMail_CreationDate DATETIME '$.Voicemail.CreationDate',
		--		CallbackRequest_Id VARCHAR(20) '$.CallbackRequest.Id',
		--		CallbackRequest_CreationDate DATETIME '$.CallbackRequest.CreationDate',
		--		CallbackRequest_QueueId INT '$.CallbackRequest.QueueId',
		--		CallbackRequest_Priority INT '$.CallbackRequest.Priority',
		--		CallbackRequest_ExtraInfo VARCHAR(20) '$.CallbackRequest.ExtraInfo',
		--		CallbackRequest_PopUrl VARCHAR(20) '$.CallbackRequest.PopUrl',
		--		CallbackRequest_DestPhoneNumber VARCHAR(20) '$.CallbackRequest.DestPhoneNumber',
		--		CallbackRequest_DestContactName VARCHAR(20) '$.CallbackRequest.DestContactName',
		--		CallbackRequest_DefaultCallerType VARCHAR(20) '$.CallbackRequest.DefaultCallerType',
		--		CallbackRequest_DefaultMainSubject VARCHAR(20) '$.CallbackRequest.DefaultMainSubject',
		--		CallbackRequest_DefaultSubsubject VARCHAR(20) '$.CallbackRequest.DefaultSubsubject',
		--		CallbackRequest_IsAnswered BIT '$.CallbackRequest.IsAnswered',
		--		CallbackRequest_DeliveryInfo_StartDate DATETIME '$.CallbackRequest.DeliveryInfo.StartDate',
		--		CallbackRequest_DeliveryInfo_EndDate DATETIME '$.CallbackRequest.DeliveryInfo.EndDate',
		--		CallbackRequest_DeliveryInfo_RetryMinutesInterval INT '$.CallbackRequest.DeliveryInfo.RetryMinutesInterval',
		--		CallbackRequest_DeliveryInfo_LastDeliveryAttemptUtc DATETIME '$.CallbackRequest.DeliveryInfo.LastDeliveryAttemptUtc',
		--		CallbackRequest_DeliveryInfo_DeliveryAttempts INT '$.CallbackRequest.DeliveryInfo.DeliveryAttempts',
		--		CallbackRequest_DeliveryInfo_ExpirationMinutes INT '$.CallbackRequest.DeliveryInfo.ExpirationMinutes',
		--		CallbackRequest_DeliveryInfo_MaxDeliveryAttempts INT '$.CallbackRequest.DeliveryInfo.MaxDeliveryAttempts',
		--		SipHeaders_HeaderName VARCHAR(20) '$.SipHeaders.HeaderName',
		--		SipHeaders_HeaderValue VARCHAR(20) '$.SipHeaders.HeaderValue',
		--		QueuingHistory_QueuingDate DATETIME '$.QueuingHistory.QueuingDate',
		--		QueuingHistory_QueueId INT '$.QueuingHistory.QueueId',
		--		QueuingHistory_ExitQueueDate DATETIME '$.QueuingHistory.ExitQueueDate',
		--		QueuingHistory_ExitCode INT '$.QueuingHistory.ExitCode',
		--		AnsweredHistory_AnsweredDate DATETIME '$.AnsweredHistory.AnsweredDate',
		--		AnsweredHistory_AgentId INT '$.AnsweredHistory.AgentId',
		--		AnsweredHistory_AgentPhoneNumber VARCHAR(20) '$.AnsweredHistory.AgentPhoneNumber',
		--		AnsweredHistory_WrapupStart DATETIME '$.AnsweredHistory.WrapupStart',
		--		AnsweredHistory_WrapupEnd DATETIME '$.AnsweredHistory.WrapupEnd',
		--		AnsweredHistory_ACWStart DATETIME '$.AnsweredHistory.ACWStart',
		--		AnsweredHistory_ACWEnd DATETIME '$.AnsweredHistory.ACWEnd',
		--		AnsweredHistory_HoldCount DATETIME '$.AnsweredHistory.HoldCount',
		--		AnsweredHistory_SecondsOnHold DATETIME '$.AnsweredHistory.SecondsOnHold',
		--		Checkpoints_Id INT '$.Checkpoints.Id',
		--		Checkpoints_CreationDate DATETIME '$.Checkpoints.CreationDate',
		--		Checkpoints_Data VARCHAR(20) '$.Checkpoints.Data',
		--		TransferAttemptsHistory_AgentSessionId VARCHAR(20) '$.TransferAttemptsHistory.AgentSessionId',
		--		TransferAttemptsHistory_StartDate DATETIME '$.TransferAttemptsHistory.StartDate',
		--		TransferAttemptsHistory_PhoneNumber VARCHAR(20) '$.TransferAttemptsHistory.PhoneNumber',
		--		TransferAttemptsHistory_CallTransferMode INT '$.TransferAttemptsHistory.CallTransferMode',
		--		TransferAttemptsHistory_EndDate DATETIME '$.TransferAttemptsHistory.EndDate',
		--		TransferAttemptsHistory_ExitCode INT '$.TransferAttemptsHistory.ExitCode',
		--		TransferAttemptsHistory_DstQueueId INT '$.TransferAttemptsHistory.DstQueueId',
		--		GeneralInfo_ThreadId VARCHAR(20) '$.GeneralInfo.ThreadId',
		--		GeneralInfo_CallId VARCHAR(20) '$.GeneralInfo.CallId',
		--		GeneralInfo_StartDate DATETIME '$.GeneralInfo.StartDate',
		--		GeneralInfo_EndDate DATETIME '$.GeneralInfo.EndDate',
		--		GeneralInfo_OrgNumber VARCHAR(20) '$.GeneralInfo.OrgNumber',
		--		GeneralInfo_OrgName VARCHAR(20) '$.GeneralInfo.OrgName',
		--		GeneralInfo_DstNumber VARCHAR(20) '$.GeneralInfo.DstNumber',
		--		GeneralInfo_DstName VARCHAR(20) '$.GeneralInfo.DstName',
		--		GeneralInfo_ExitCode INT '$.GeneralInfo.ExitCode',
		--		GeneralInfo_OriginationReason INT '$.GeneralInfo.OriginationReason',
		--		GeneralInfo_HangupEvtRd BIT '$.GeneralInfo.HangupEvtRd',
		--		GeneralInfo_Label VARCHAR(20) '$.GeneralInfo.Label',
		--		GeneralInfo_Direction INT '$.GeneralInfo.Direction',
		--		Disposition_ClientType VARCHAR(20) '$.Disposition.ClientType',
		--		Disposition_ClientTypeRef1 VARCHAR(20) '$.Disposition.ClientTypeRef1',
		--		Disposition_ClientTypeRef2 VARCHAR(20) '$.Disposition.ClientTypeRef2',
		--		Disposition_ClientTypeRef3 VARCHAR(20) '$.Disposition.ClientTypeRef3',
		--		Disposition_MainSubject VARCHAR(20) '$.Disposition.MainSubject',
		--		Disposition_Subsubject VARCHAR(20) '$.Disposition.Subsubject',
		--		Disposition_SubsubjectDetails VARCHAR(20) '$.Disposition.SubsubjectDetails',
		--		Disposition_Resolution VARCHAR(20) '$.Disposition.Resolution',
		--		Disposition_IsFlagged BIT '$.Disposition.IsFlagged',
		--		Disposition_FlagingReason VARCHAR(20) '$.Disposition.FlagingReason',
		--		Disposition_Notes VARCHAR(20) '$.Disposition.Notes',
		--		Disposition_CreationDate DATETIME '$.Disposition.CreationDate',
		--		Disposition_CreatedBy INT '$.Disposition.CreatedBy'
		--		);	


		--		INSERT INTO [Import].[CallsReceived]
		--		(
		--			IvrId,
		--			VoiceMail_ID,
		--			VoiceMail_ReferenceNo,
		--			VoiceMail_SecondsDuration,
		--			VoiceMail_EntryQueueId,
		--			VoiceMail_CreationDate,
		--			CallbackRequest_Id,
		--			CallbackRequest_CreationDate,
		--			CallbackRequest_QueueId,
		--			CallbackRequest_Priority,
		--			CallbackRequest_ExtraInfo,
		--			CallbackRequest_PopUrl,
		--			CallbackRequest_DestPhoneNumber,
		--			CallbackRequest_DestContactName,
		--			CallbackRequest_DefaultCallerType,
		--			CallbackRequest_DefaultMainSubject,
		--			CallbackRequest_DefaultSubsubject,
		--			CallbackRequest_IsAnswered,
		--			CallbackRequest_DeliveryInfo_StartDate,
		--			CallbackRequest_DeliveryInfo_EndDate,
		--			CallbackRequest_DeliveryInfo_RetryMinutesInterval,
		--			CallbackRequest_DeliveryInfo_LastDeliveryAttemptUtc,
		--			CallbackRequest_DeliveryInfo_DeliveryAttempts,
		--			CallbackRequest_DeliveryInfo_ExpirationMinutes,
		--			CallbackRequest_DeliveryInfo_MaxDeliveryAttempts,
		--			SipHeaders_HeaderName,
		--			SipHeaders_HeaderValue,
		--			QueuingHistory_QueuingDate,
		--			QueuingHistory_QueueId,
		--			QueuingHistory_ExitQueueDate,
		--			QueuingHistory_ExitCode,
		--			AnsweredHistory_AnsweredDate,
		--			AnsweredHistory_AgentId,
		--			AnsweredHistory_AgentPhoneNumber,
		--			AnsweredHistory_WrapupStart,
		--			AnsweredHistory_WrapupEnd,
		--			AnsweredHistory_ACWStart,
		--			AnsweredHistory_ACWEnd,
		--			AnsweredHistory_HoldCount,
		--			AnsweredHistory_SecondsOnHold,
		--			Checkpoints_Id,
		--			Checkpoints_CreationDate,
		--			Checkpoints_Data,
		--			TransferAttemptsHistory_AgentSessionId,
		--			TransferAttemptsHistory_StartDate,
		--			TransferAttemptsHistory_PhoneNumber,
		--			TransferAttemptsHistory_CallTransferMode,
		--			TransferAttemptsHistory_EndDate,
		--			TransferAttemptsHistory_ExitCode,
		--			TransferAttemptsHistory_DstQueueId,
		--			GeneralInfo_ThreadId,
		--			GeneralInfo_CallId,
		--			GeneralInfo_StartDate,
		--			GeneralInfo_EndDate,
		--			GeneralInfo_OrgNumber,
		--			GeneralInfo_OrgName,
		--			GeneralInfo_DstNumber,
		--			GeneralInfo_DstName,
		--			GeneralInfo_ExitCode,
		--			GeneralInfo_OriginationReason,
		--			GeneralInfo_HangupEvtRd,
		--			GeneralInfo_Label,
		--			GeneralInfo_Direction,
		--			Disposition_ClientType,
		--			Disposition_ClientTypeRef1,
		--			Disposition_ClientTypeRef2,
		--			Disposition_ClientTypeRef3,
		--			Disposition_MainSubject,
		--			Disposition_Subsubject,
		--			Disposition_SubsubjectDetails,
		--			Disposition_Resolution,
		--			Disposition_IsFlagged,
		--			Disposition_FlagingReason,
		--			Disposition_Notes,
		--			Disposition_CreationDate,
		--			Disposition_CreatedBy
		--		)
		--	SELECT
		--		IvrId,
		--		VoiceMail_ID,
		--		VoiceMail_ReferenceNo,
		--		VoiceMail_SecondsDuration,
		--		VoiceMail_EntryQueueId,
		--		VoiceMail_CreationDate,
		--		CallbackRequest_Id,
		--		CallbackRequest_CreationDate,
		--		CallbackRequest_QueueId,
		--		CallbackRequest_Priority,
		--		CallbackRequest_ExtraInfo,
		--		CallbackRequest_PopUrl,
		--		CallbackRequest_DestPhoneNumber,
		--		CallbackRequest_DestContactName,
		--		CallbackRequest_DefaultCallerType,
		--		CallbackRequest_DefaultMainSubject,
		--		CallbackRequest_DefaultSubsubject,
		--		CallbackRequest_IsAnswered,
		--		CallbackRequest_DeliveryInfo_StartDate,
		--		CallbackRequest_DeliveryInfo_EndDate,
		--		CallbackRequest_DeliveryInfo_RetryMinutesInterval,
		--		CallbackRequest_DeliveryInfo_LastDeliveryAttemptUtc,
		--		CallbackRequest_DeliveryInfo_DeliveryAttempts,
		--		CallbackRequest_DeliveryInfo_ExpirationMinutes,
		--		CallbackRequest_DeliveryInfo_MaxDeliveryAttempts,
		--		SipHeaders_HeaderName,
		--		SipHeaders_HeaderValue,
		--		QueuingHistory_QueuingDate,
		--		QueuingHistory_QueueId,
		--		QueuingHistory_ExitQueueDate,
		--		QueuingHistory_ExitCode,
		--		AnsweredHistory_AnsweredDate,
		--		AnsweredHistory_AgentId,
		--		AnsweredHistory_AgentPhoneNumber,
		--		AnsweredHistory_WrapupStart,
		--		AnsweredHistory_WrapupEnd,
		--		AnsweredHistory_ACWStart,
		--		AnsweredHistory_ACWEnd,
		--		AnsweredHistory_HoldCount,
		--		AnsweredHistory_SecondsOnHold,
		--		Checkpoints_Id,
		--		Checkpoints_CreationDate,
		--		Checkpoints_Data,
		--		TransferAttemptsHistory_AgentSessionId,
		--		TransferAttemptsHistory_StartDate,
		--		TransferAttemptsHistory_PhoneNumber,
		--		TransferAttemptsHistory_CallTransferMode,
		--		TransferAttemptsHistory_EndDate,
		--		TransferAttemptsHistory_ExitCode,
		--		TransferAttemptsHistory_DstQueueId,
		--		GeneralInfo_ThreadId,
		--		GeneralInfo_CallId,
		--		GeneralInfo_StartDate,
		--		GeneralInfo_EndDate,
		--		GeneralInfo_OrgNumber,
		--		GeneralInfo_OrgName,
		--		GeneralInfo_DstNumber,
		--		GeneralInfo_DstName,
		--		GeneralInfo_ExitCode,
		--		GeneralInfo_OriginationReason,
		--		GeneralInfo_HangupEvtRd,
		--		GeneralInfo_Label,
		--		GeneralInfo_Direction,
		--		Disposition_ClientType,
		--		Disposition_ClientTypeRef1,
		--		Disposition_ClientTypeRef2,
		--		Disposition_ClientTypeRef3,
		--		Disposition_MainSubject,
		--		Disposition_Subsubject,
		--		Disposition_SubsubjectDetails,
		--		Disposition_Resolution,
		--		Disposition_IsFlagged,
		--		Disposition_FlagingReason,
		--		Disposition_Notes,
		--		Disposition_CreationDate,
		--		Disposition_CreatedBy
		--	FROM 
		--		#temp_CallReceived
		--	EXCEPT
		--	SELECT
		--		IvrId,
		--		VoiceMail_ID,
		--		VoiceMail_ReferenceNo,
		--		VoiceMail_SecondsDuration,
		--		VoiceMail_EntryQueueId,
		--		VoiceMail_CreationDate,
		--		CallbackRequest_Id,
		--		CallbackRequest_CreationDate,
		--		CallbackRequest_QueueId,
		--		CallbackRequest_Priority,
		--		CallbackRequest_ExtraInfo,
		--		CallbackRequest_PopUrl,
		--		CallbackRequest_DestPhoneNumber,
		--		CallbackRequest_DestContactName,
		--		CallbackRequest_DefaultCallerType,
		--		CallbackRequest_DefaultMainSubject,
		--		CallbackRequest_DefaultSubsubject,
		--		CallbackRequest_IsAnswered,
		--		CallbackRequest_DeliveryInfo_StartDate,
		--		CallbackRequest_DeliveryInfo_EndDate,
		--		CallbackRequest_DeliveryInfo_RetryMinutesInterval,
		--		CallbackRequest_DeliveryInfo_LastDeliveryAttemptUtc,
		--		CallbackRequest_DeliveryInfo_DeliveryAttempts,
		--		CallbackRequest_DeliveryInfo_ExpirationMinutes,
		--		CallbackRequest_DeliveryInfo_MaxDeliveryAttempts,
		--		SipHeaders_HeaderName,
		--		SipHeaders_HeaderValue,
		--		QueuingHistory_QueuingDate,
		--		QueuingHistory_QueueId,
		--		QueuingHistory_ExitQueueDate,
		--		QueuingHistory_ExitCode,
		--		AnsweredHistory_AnsweredDate,
		--		AnsweredHistory_AgentId,
		--		AnsweredHistory_AgentPhoneNumber,
		--		AnsweredHistory_WrapupStart,
		--		AnsweredHistory_WrapupEnd,
		--		AnsweredHistory_ACWStart,
		--		AnsweredHistory_ACWEnd,
		--		AnsweredHistory_HoldCount,
		--		AnsweredHistory_SecondsOnHold,
		--		Checkpoints_Id,
		--		Checkpoints_CreationDate,
		--		Checkpoints_Data,
		--		TransferAttemptsHistory_AgentSessionId,
		--		TransferAttemptsHistory_StartDate,
		--		TransferAttemptsHistory_PhoneNumber,
		--		TransferAttemptsHistory_CallTransferMode,
		--		TransferAttemptsHistory_EndDate,
		--		TransferAttemptsHistory_ExitCode,
		--		TransferAttemptsHistory_DstQueueId,
		--		GeneralInfo_ThreadId,
		--		GeneralInfo_CallId,
		--		GeneralInfo_StartDate,
		--		GeneralInfo_EndDate,
		--		GeneralInfo_OrgNumber,
		--		GeneralInfo_OrgName,
		--		GeneralInfo_DstNumber,
		--		GeneralInfo_DstName,
		--		GeneralInfo_ExitCode,
		--		GeneralInfo_OriginationReason,
		--		GeneralInfo_HangupEvtRd,
		--		GeneralInfo_Label,
		--		GeneralInfo_Direction,
		--		Disposition_ClientType,
		--		Disposition_ClientTypeRef1,
		--		Disposition_ClientTypeRef2,
		--		Disposition_ClientTypeRef3,
		--		Disposition_MainSubject,
		--		Disposition_Subsubject,
		--		Disposition_SubsubjectDetails,
		--		Disposition_Resolution,
		--		Disposition_IsFlagged,
		--		Disposition_FlagingReason,
		--		Disposition_Notes,
		--		Disposition_CreationDate,
		--		Disposition_CreatedBy
		--	FROM
		--		 [Import].[CallsReceived];	
		--COMMIT TRAN

