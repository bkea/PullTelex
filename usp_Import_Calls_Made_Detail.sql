USE [Call_DW]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF OBJECT_ID('Import.usp_Import_Calls_Made_Detail', 'P') IS NOT NULL
    DROP PROCEDURE Import.usp_Import_Calls_Made_Detail
GO

CREATE PROCEDURE [Import].[usp_Import_Calls_Made_Detail]
(
	@json NVARCHAR(MAX)
)
AS
-- =============================================
-- Author:		Bill Kea
-- Create date: 08/21/2021
-- Description:	Imports JSON documents into Import.CallsMade table
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

	DROP TABLE IF EXISTS #temp_CallsMade

RETRYINSERT:
BEGIN TRAN
	BEGIN TRY
		SELECT
			Id,
			AgentId,
			AgentSessionId,
			ExitCode,
			StartDate,
			WrapupStart,
			WrapupEnd,
			ACWStart,
			ACWEnd,
			HoldCount,
			SecondsOnHold,
			CallBackId
		INTO
			#temp_CallsMade_Detail
		FROM OPENJSON (@json)
			WITH(
				Id VARCHAR(20) '$.Id',
				AgentId INT '$.AgentId',
				AgentSessionId VARCHAR(20) '$.AgentSessionId',
				ExitCode INT '$.ExitCode',
				StartDate DATETIME '$.StartDate',
				WrapupStart DATETIME '$.WrapupStart',
				WrapupEnd DATETIME '$.WrapupEnd',
				ACWStart VARCHAR(20) '$.ACWStart',
				ACWEnd VARCHAR(20) '$.ACWEnd',
				HoldCount INT '$.HoldCount',
				SecondsOnHold INT '$.SecondsOnHold',
				CallBackId VARCHAR(20) '$.CallBackId'
				)


		INSERT INTO [Import].[CallsMade_Detail]
			(
			Id,
			AgentId,
			AgentSessionId,
			ExitCode,
			StartDate,
			WrapupStart,
			WrapupEnd,
			ACWStart,
			ACWEnd,
			HoldCount,
			SecondsOnHold,
			CallBackId
			)
		SELECT
			Id,
			AgentId,
			AgentSessionId,
			ExitCode,
			StartDate,
			WrapupStart,
			WrapupEnd,
			ACWStart,
			ACWEnd,
			HoldCount,
			SecondsOnHold,
			CallBackId
		FROM 
			#temp_CallsMade_Detail
		EXCEPT
		SELECT
			Id,
			AgentId,
			AgentSessionId,
			ExitCode,
			StartDate,
			WrapupStart,
			WrapupEnd,
			ACWStart,
			ACWEnd,
			HoldCount,
			SecondsOnHold,
			CallBackId
		FROM
				[Import].[CallsMade_Detail];	
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


