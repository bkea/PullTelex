USE [Call_DW]
GO

/****** Object:  StoredProcedure [Import].[usp_Import_Calls_Received_AnsweredHistory]    Script Date: 9/16/2021 4:24:39 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF OBJECT_ID('Import.usp_Import_Calls_Received_AnsweredHistory', 'P') IS NOT NULL
    DROP PROCEDURE Import.usp_Import_Calls_Received_AnsweredHistory
GO

CREATE PROCEDURE [Import].[usp_Import_Calls_Received_AnsweredHistory]
(
	@json NVARCHAR(MAX)
)
AS
-- =============================================
-- Author:		Bill Kea
-- Create date: 08/21/2021
-- Description:	Imports JSON documents into Import_CallsReceived_AnsweredHistory table
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
SELECT
		IvrId,
		GeneralInfo_CallId,
		AnsweredHistory_AnsweredDate,
		AnsweredHistory_AgentId,
		AnsweredHistory_AgentPhoneNumber,
		AnsweredHistory_WrapupStart,
		AnsweredHistory_WrapupEnd,
		AnsweredHistory_ACWStart,
		AnsweredHistory_ACWEnd,
		AnsweredHistory_HoldCount,
		AnsweredHistory_SecondsOnHold
	INTO 
		#CallsReceived_AnsweredHistory
	FROM OPENJSON(@json)
		WITH (
		IvrId INT '$.IvrId',
		GeneralInfo_CallId VARCHAR(20) '$.GeneralInfo.CallId',
		AnsweredHistory_AnsweredDate DATETIME '$.AnsweredHistory.AnsweredDate',
		AnsweredHistory_AgentId INT '$.AnsweredHistory.AgentId',
		AnsweredHistory_AgentPhoneNumber VARCHAR(20) '$.AnsweredHistory.AgentPhoneNumber',
		AnsweredHistory_WrapupStart DATETIME '$.AnsweredHistory.WrapupStart',
		AnsweredHistory_WrapupEnd DATETIME '$.AnsweredHistory.WrapupEnd',
		AnsweredHistory_ACWStart DATETIME '$.AnsweredHistory.ACWStart',
		AnsweredHistory_ACWEnd DATETIME '$.AnsweredHistory.ACWEnd',
		AnsweredHistory_HoldCount DATETIME '$.AnsweredHistory.HoldCount',
		AnsweredHistory_SecondsOnHold DATETIME '$.AnsweredHistory.SecondsOnHold'
		)
	WHERE
		AnsweredHistory_AgentId IS NOT NULL;

			INSERT INTO
		[Import].[CallsReceived_AnsweredHistory]
		(
			IvrId,
			GeneralInfo_CallId,
			AnsweredHistory_AnsweredDate,
			AnsweredHistory_AgentId,
			AnsweredHistory_AgentPhoneNumber,
			AnsweredHistory_WrapupStart,
			AnsweredHistory_WrapupEnd,
			AnsweredHistory_ACWStart,
			AnsweredHistory_ACWEnd,
			AnsweredHistory_HoldCount,
			AnsweredHistory_SecondsOnHold
		)
			SELECT
				IvrId,
				GeneralInfo_CallId,
				AnsweredHistory_AnsweredDate,
				AnsweredHistory_AgentId,
				AnsweredHistory_AgentPhoneNumber,
				AnsweredHistory_WrapupStart,
				AnsweredHistory_WrapupEnd,
				AnsweredHistory_ACWStart,
				AnsweredHistory_ACWEnd,
				AnsweredHistory_HoldCount,
				AnsweredHistory_SecondsOnHold
			FROM
				#CallsReceived_AnsweredHistory
			EXCEPT
			SELECT
				IvrId,
				GeneralInfo_CallId,
				AnsweredHistory_AnsweredDate,
				AnsweredHistory_AgentId,
				AnsweredHistory_AgentPhoneNumber,
				AnsweredHistory_WrapupStart,
				AnsweredHistory_WrapupEnd,
				AnsweredHistory_ACWStart,
				AnsweredHistory_ACWEnd,
				AnsweredHistory_HoldCount,
				AnsweredHistory_SecondsOnHold
			FROM
				[Import].[CallsReceived_AnsweredHistory]

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