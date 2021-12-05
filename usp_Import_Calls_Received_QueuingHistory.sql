USE [Call_DW]
GO

/****** Object:  StoredProcedure [Import].[usp_Import_Calls_Received_QueuingHistory]    Script Date: 9/16/2021 4:24:39 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF OBJECT_ID('Import.usp_Import_Calls_Received_QueuingHistory', 'P') IS NOT NULL
    DROP PROCEDURE Import.usp_Import_Calls_Received_QueuingHistory
GO

CREATE PROCEDURE [Import].[usp_Import_Calls_Received_QueuingHistory]
(
	@json NVARCHAR(MAX)
)
AS
-- =============================================
-- Author:		Bill Kea
-- Create date: 08/21/2021
-- Description:	Imports JSON documents into Import_Calls_Received_QueuingHistory table
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
		QueuingHistory_QueuingDate,
		QueuingHistory_QueueId,
		QueuingHistory_ExitQueueDate,
		QueuingHistory_ExitCode
	INTO 
		#CallsReceived_QueuingHistory
	FROM OPENJSON(@json)
		WITH (
		IvrId INT '$.IvrId',
		GeneralInfo_CallId VARCHAR(20) '$.GeneralInfo.CallId',
		QueuingHistory_QueuingDate DATETIME '$.QueuingHistory.QueuingDate',
		QueuingHistory_QueueId INT '$.QueuingHistory.QueueId',
		QueuingHistory_ExitQueueDate DATETIME '$.QueuingHistory.ExitQueueDate',
		QueuingHistory_ExitCode INT '$.QueuingHistory.ExitCode'
		)
	WHERE
		QueuingHistory_QueueId IS NOT NULL
	

	INSERT INTO [Import].[CallsReceived_QueuingHistory]
		(
		IvrId,
		GeneralInfo_CallId,
		QueuingHistory_QueuingDate,
		QueuingHistory_QueueId,
		QueuingHistory_ExitQueueDate,
		QueuingHistory_ExitCode
		)
		SELECT 
			IvrId,
			GeneralInfo_CallId,
			QueuingHistory_QueuingDate,
			QueuingHistory_QueueId,
			QueuingHistory_ExitQueueDate,
			QueuingHistory_ExitCode
		FROM
			#CallsReceived_QueuingHistory
		EXCEPT
		SELECT
			IvrId,
			GeneralInfo_CallId,
			QueuingHistory_QueuingDate,
			QueuingHistory_QueueId,
			QueuingHistory_ExitQueueDate,
			QueuingHistory_ExitCode
		FROM
			[Import].[CallsReceived_QueuingHistory]

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