USE [Call_DW]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF OBJECT_ID('Import.usp_Import_Calls_Made_Disposition', 'P') IS NOT NULL
    DROP PROCEDURE Import.usp_Import_Calls_Made_Disposition
GO

CREATE PROCEDURE [Import].[usp_Import_Calls_Made_Disposition]
(
	@json NVARCHAR(MAX)
)
AS
-- =============================================
-- Author:		Bill Kea
-- Create date: 08/21/2021
-- Description:	Imports JSON documents into Import.CallsMade_Disposition table
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

	DROP TABLE IF EXISTS #temp_CallsMade_Dispostion

RETRYINSERT:
BEGIN TRAN
	BEGIN TRY
			SELECT
				Id,
				Disposition_ClientType,
				Disposition_ClientTypeRef1,
				Disposition_ClientTypeRef2,
				Disposition_ClientTypeRef3,
				Disposition_MainSubject,
				Disposition_Subsubject,
				Disposition_SubsubjectDetails,
				Disposition_Resolution,
				Disposition_IsFlagged,
				Disposition_FlagingReason,
				Disposition_Notes,
				Disposition_CreationDate,
				Disposition_CreatedBy
			INTO
				#temp_CallsMade_Dispostion
			FROM OPENJSON (@json)
				WITH(
					Id VARCHAR(20) '$.Id',
					Disposition_ClientType VARCHAR(20) '$.Disposition.ClientType',
					Disposition_ClientTypeRef1 VARCHAR(20) '$.Disposition.ClientTypeRef1',
					Disposition_ClientTypeRef2 VARCHAR(20) '$.Disposition.ClientTypeRef2',
					Disposition_ClientTypeRef3 VARCHAR(20) '$.Disposition.ClientTypeRef3',
					Disposition_MainSubject VARCHAR(20) '$.Disposition.MainSubject',
					Disposition_Subsubject VARCHAR(20) '$.Disposition.Subsubject',
					Disposition_SubsubjectDetails VARCHAR(20) '$.Disposition.SubsubjectDetails',
					Disposition_Resolution VARCHAR(20) '$.Disposition.Resolution',
					Disposition_IsFlagged BIT '$.Disposition.IsFlagged',
					Disposition_FlagingReason VARCHAR(20) '$.Disposition.FlagingReason',
					Disposition_Notes VARCHAR(20) '$.Disposition.Notes',
					Disposition_CreationDate DATETIME '$.Disposition.CreationDate',
					Disposition_CreatedBy INT '$.Disposition.CreatedBy'
					)
			WHERE 
				Disposition_CreationDate IS NOT NULL


			INSERT INTO [Import].[CallsMade_Disposition]
				(
				Id,
				Disposition_ClientType,
				Disposition_ClientTypeRef1,
				Disposition_ClientTypeRef2,
				Disposition_ClientTypeRef3,
				Disposition_MainSubject,
				Disposition_Subsubject,
				Disposition_SubsubjectDetails,
				Disposition_Resolution,
				Disposition_IsFlagged,
				Disposition_FlagingReason,
				Disposition_Notes,
				Disposition_CreationDate,
				Disposition_CreatedBy
				)
			SELECT
				Id,
				Disposition_ClientType,
				Disposition_ClientTypeRef1,
				Disposition_ClientTypeRef2,
				Disposition_ClientTypeRef3,
				Disposition_MainSubject,
				Disposition_Subsubject,
				Disposition_SubsubjectDetails,
				Disposition_Resolution,
				Disposition_IsFlagged,
				Disposition_FlagingReason,
				Disposition_Notes,
				Disposition_CreationDate,
				Disposition_CreatedBy
			FROM 
				#temp_CallsMade_Dispostion
			EXCEPT
			SELECT
				Id,
				Disposition_ClientType,
				Disposition_ClientTypeRef1,
				Disposition_ClientTypeRef2,
				Disposition_ClientTypeRef3,
				Disposition_MainSubject,
				Disposition_Subsubject,
				Disposition_SubsubjectDetails,
				Disposition_Resolution,
				Disposition_IsFlagged,
				Disposition_FlagingReason,
				Disposition_Notes,
				Disposition_CreationDate,
				Disposition_CreatedBy
			FROM
				 [Import].[CallsMade_Disposition];	

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


