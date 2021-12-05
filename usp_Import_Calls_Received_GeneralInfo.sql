USE [Call_DW]
GO

/****** Object:  StoredProcedure [Import].[usp_Import_Calls_Received_GeneralInfo]    Script Date: 9/16/2021 4:24:39 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF OBJECT_ID('Import.usp_Import_Calls_Received_GeneralInfo', 'P') IS NOT NULL
    DROP PROCEDURE Import.usp_Import_Calls_Received_GeneralInfo
GO

CREATE PROCEDURE [Import].[usp_Import_Calls_Received_GeneralInfo]
(
	@json NVARCHAR(MAX)
)
AS
-- =============================================
-- Author:		Bill Kea
-- Create date: 08/21/2021
-- Description:	Imports JSON documents into Import_CallsReceived_GeneralInfo table
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
	GeneralInfo_ThreadId,
	GeneralInfo_CallId,
	GeneralInfo_StartDate = TRY_PARSE(GeneralInfo_StartDate AS DATETIME USING 'en-us'),
	GeneralInfo_EndDate = TRY_PARSE(GeneralInfo_EndDate AS DATETIME USING 'en-us'),
	GeneralInfo_OrgNumber,
	GeneralInfo_OrgName,
	GeneralInfo_DstNumber,
	GeneralInfo_DstName,
	GeneralInfo_ExitCode,
	GeneralInfo_OriginationReason,
	GeneralInfo_HangupEvtRd,
	GeneralInfo_Label,
	GeneralInfo_Direction
INTO
	#CallsReceived_GeneralInfo
FROM OPENJSON(@json)
	WITH (
	IvrId INT '$.IvrId',
	GeneralInfo_ThreadId VARCHAR(20) '$.GeneralInfo.ThreadId',
	GeneralInfo_CallId VARCHAR(20) '$.GeneralInfo.CallId',
	GeneralInfo_StartDate VARCHAR(50) '$.GeneralInfo.StartDate',
	GeneralInfo_EndDate VARCHAR(50) '$.GeneralInfo.EndDate',
	GeneralInfo_OrgNumber VARCHAR(20) '$.GeneralInfo.OrgNumber',
	GeneralInfo_OrgName VARCHAR(20) '$.GeneralInfo.OrgName',
	GeneralInfo_DstNumber VARCHAR(20) '$.GeneralInfo.DstNumber',
	GeneralInfo_DstName VARCHAR(20) '$.GeneralInfo.DstName',
	GeneralInfo_ExitCode INT '$.GeneralInfo.ExitCode',
	GeneralInfo_OriginationReason INT '$.GeneralInfo.OriginationReason',
	GeneralInfo_HangupEvtRd BIT '$.GeneralInfo.HangupEvtRd',
	GeneralInfo_Label VARCHAR(20) '$.GeneralInfo.Label',
	GeneralInfo_Direction INT '$.GeneralInfo.Direction'
	)
WHERE
	GeneralInfo_ThreadId IS NOT NULL;



INSERT INTO
	[Import].[CallsReceived_GeneralInfo]
	(
		IvrId,
		GeneralInfo_ThreadId,
		GeneralInfo_CallId,
		GeneralInfo_StartDate,
		GeneralInfo_EndDate,
		GeneralInfo_OrgNumber,
		GeneralInfo_OrgName,
		GeneralInfo_DstNumber,
		GeneralInfo_DstName,
		GeneralInfo_ExitCode,
		GeneralInfo_OriginationReason,
		GeneralInfo_HangupEvtRd,
		GeneralInfo_Label,
		GeneralInfo_Direction
	)
SELECT
	IvrId,
	GeneralInfo_ThreadId,
	GeneralInfo_CallId,
	GeneralInfo_StartDate,
	GeneralInfo_EndDate,
	GeneralInfo_OrgNumber,
	GeneralInfo_OrgName,
	GeneralInfo_DstNumber,
	GeneralInfo_DstName,
	GeneralInfo_ExitCode,
	GeneralInfo_OriginationReason,
	GeneralInfo_HangupEvtRd,
	GeneralInfo_Label,
	GeneralInfo_Direction
FROM
	#CallsReceived_GeneralInfo
EXCEPT
SELECT
	IvrId,
	GeneralInfo_ThreadId,
	GeneralInfo_CallId,
	GeneralInfo_StartDate,
	GeneralInfo_EndDate,
	GeneralInfo_OrgNumber,
	GeneralInfo_OrgName,
	GeneralInfo_DstNumber,
	GeneralInfo_DstName,
	GeneralInfo_ExitCode,
	GeneralInfo_OriginationReason,
	GeneralInfo_HangupEvtRd,
	GeneralInfo_Label,
	GeneralInfo_Direction
FROM
	[Import].[CallsReceived_GeneralInfo]
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