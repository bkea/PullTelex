USE Call_DW

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

IF OBJECT_ID('usp_Import_AgentStatus', 'P') IS NOT NULL
    DROP PROCEDURE usp_Import_AgentStatus
GO

CREATE PROCEDURE usp_Import_AgentStatus
(
	@json NVARCHAR(MAX)
)
AS
-- =============================================
-- Author:		Bill Kea
-- Create date: 08/21/2021
-- Description:	Imports JSON documents into Import.AgantStatus table
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
	--test to see if parameter contains JSON documents
		IF (ISJSON(@json) = 0) 	
		BEGIN
			RAISERROR ('Invalid or empty Agent Status JSON document',11,1) WITH NOWAIT;
		END

		BEGIN TRAN
			SELECT 
				AgentId,
				SessionId,
				StatusCode,
				StatusDesc,
				ChangeReason,
				[Availability],
				DateChanged,
				Duration
			INTO
				#temp_agentstatus
			FROM OPENJSON(@json)
			  WITH (
				AgentId INT 'strict $.AgentId',
				SessionId VARCHAR(20) '$.SessionId',
				StatusCode INT '$.StatusCode',
				StatusDesc VARCHAR(1000) '$.StatusDesc',
				ChangeReason VARCHAR(100) '$.ChangeReason',
				[Availability] BIT '$.Availability',
				DateChanged VARCHAR(20) '$.DateChanged',
				Duration INT '$.Duration'
			  );

			INSERT INTO 
				[Import].[AgentStatus] (AgentId, SessionId, StatusCode, StatusDesc, ChangeReason, [Availability], DateChanged, Duration)
			SELECT 
				AgentId,
				SessionId,
				StatusCode,
				StatusDesc,
				ChangeReason,
				[Availability],
				DateChanged,
				Duration
			FROM 
				#temp_agentstatus
			EXCEPT
			SELECT 
				AgentId,
				SessionId,
				StatusCode,
				StatusDesc,
				ChangeReason,
				[Availability],
				DateChanged,
				Duration
			FROM 
				[Import].[AgentStatus];
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

