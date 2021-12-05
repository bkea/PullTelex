USE [Call_DW]
GO

/****** Object:  StoredProcedure [Import].[usp_Import_Calls_Received_TransferAttemptsHistory]    Script Date: 9/16/2021 4:24:39 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF OBJECT_ID('Import.usp_Import_Calls_Received_TransferAttemptsHistory', 'P') IS NOT NULL
    DROP PROCEDURE Import.usp_Import_Calls_Received_TransferAttemptsHistory
GO

CREATE PROCEDURE [Import].[usp_Import_Calls_Received_TransferAttemptsHistory]
(
	@json NVARCHAR(MAX)
)
AS
-- =============================================
-- Author:		Bill Kea
-- Create date: 08/21/2021
-- Description:	Imports JSON documents into Import_CallsReceived_TransferAttemptsHistory table
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
SELECT
	IvrId,
	GeneralInfo_CallId,
	TransferAttemptsHistory_AgentSessionId,
	TransferAttemptsHistory_StartDate,
	TransferAttemptsHistory_PhoneNumber,
	TransferAttemptsHistory_CallTransferMode,
	TransferAttemptsHistory_EndDate,
	TransferAttemptsHistory_ExitCode,
	TransferAttemptsHistory_DstQueueId
INTO
	#CallsReceived_TransferAttemptsHistory
FROM OPENJSON(@json)
	WITH (
	IvrId INT '$.IvrId',
	GeneralInfo_CallId VARCHAR(20) '$.GeneralInfo.CallId',
	TransferAttemptsHistory_AgentSessionId VARCHAR(20) '$.TransferAttemptsHistory.AgentSessionId',
	TransferAttemptsHistory_StartDate DATETIME '$.TransferAttemptsHistory.StartDate',
	TransferAttemptsHistory_PhoneNumber VARCHAR(20) '$.TransferAttemptsHistory.PhoneNumber',
	TransferAttemptsHistory_CallTransferMode INT '$.TransferAttemptsHistory.CallTransferMode',
	TransferAttemptsHistory_EndDate DATETIME '$.TransferAttemptsHistory.EndDate',
	TransferAttemptsHistory_ExitCode INT '$.TransferAttemptsHistory.ExitCode',
	TransferAttemptsHistory_DstQueueId INT '$.TransferAttemptsHistory.DstQueueId'
	)
WHERE
	TransferAttemptsHistory_AgentSessionId IS NOT NULL;

INSERT INTO
	[Import].[CallsReceived_TransferAttemptsHistory]
	(
	IvrId,
	GeneralInfo_CallId,
	TransferAttemptsHistory_AgentSessionId,
	TransferAttemptsHistory_StartDate,
	TransferAttemptsHistory_PhoneNumber,
	TransferAttemptsHistory_CallTransferMode,
	TransferAttemptsHistory_EndDate,
	TransferAttemptsHistory_ExitCode,
	TransferAttemptsHistory_DstQueueId
	)
SELECT
	IvrId,
	GeneralInfo_CallId,
	TransferAttemptsHistory_AgentSessionId,
	TransferAttemptsHistory_StartDate,
	TransferAttemptsHistory_PhoneNumber,
	TransferAttemptsHistory_CallTransferMode,
	TransferAttemptsHistory_EndDate,
	TransferAttemptsHistory_ExitCode,
	TransferAttemptsHistory_DstQueueId
FROM
	#CallsReceived_TransferAttemptsHistory
EXCEPT
SELECT
	IvrId,
	GeneralInfo_CallId,
	TransferAttemptsHistory_AgentSessionId,
	TransferAttemptsHistory_StartDate,
	TransferAttemptsHistory_PhoneNumber,
	TransferAttemptsHistory_CallTransferMode,
	TransferAttemptsHistory_EndDate,
	TransferAttemptsHistory_ExitCode,
	TransferAttemptsHistory_DstQueueId
FROM
	[Import].[CallsReceived_TransferAttemptsHistory]


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