CREATE PROCEDURE [dbo].[context_info_set]
	 @err_code		INT					OUTPUT
	,@err_msg		NVARCHAR(4000)		OUTPUT
	,@sid			SQL_VARIANT	=	''			--	BIGINT
	,@sp			SQL_VARIANT	=	0			--	NVARCHAR(36)
	,@usr			SQL_VARIANT	=	''			--	INT
	,@own			SQL_VARIANT	=	''			--	INT
	,@reset_context	BIT
AS
BEGIN

	/**
	*	Sets context info in the session.
	*	Used in triggers and xEvents
	*/

	SELECT
		 @err_code	=	0
		,@err_msg	=	SPACE(0);
		
	BEGIN TRY
	

		DECLARE @context_bin	VARBINARY(128);		
		DECLARE @context_str	NVARCHAR(128)	=	SPACE(0);		


		/**		
		*	Session params(semicolon separated):
		*	sid=15;sp=owner_config__update;usr=15;		
		*	sid	-	binary checksum of sid from session table
		*	sp	-	procedure name
		*	usr	-	user id
		*/

		IF @reset_context = 0
		BEGIN
					
			DECLARE @context_info TABLE(
				 [param]	NVARCHAR(128)
				,[value]	NVARCHAR(128)
			);

			INSERT INTO @context_info(
				 [param]
				,[value]
			)
			SELECT
				 [param]
				,[value]
			FROM [dbo].[context_info__get]();

		
			
			
			IF SQL_VARIANT_PROPERTY(@usr,	'BaseType')	= 'int'
			BEGIN

				MERGE @context_info AS [trg] USING(
					SELECT
						 [param]	=	'usr'
						,[value]	=	CONVERT(INT, @usr)				
				)	AS	[src](
					 [param]
					,[value]
				)
				ON	[trg].[param]	=	[src].[param]
				WHEN MATCHED THEN UPDATE SET
					 [trg].[value] = [src].[value]
				WHEN NOT MATCHED THEN INSERT(
					 [param]
					,[value]
				)
				VALUES
				(
					 [src].[param]
					,[src].[value]
				);	

			END;				
		
			
			IF SQL_VARIANT_PROPERTY(@sid,	'BaseType')	= 'bigint'
			BEGIN

				MERGE @context_info AS [trg] USING(
					SELECT
						 [param]	=	'sid'
						,[value]	=	CONVERT(BIGINT, @sid)				
				)	AS	[src]
				(
					 [param]
					,[value]
				)
				ON	[trg].[param]	=	[src].[param]
				WHEN MATCHED THEN UPDATE SET
					 [trg].[value] = [src].[value]
				WHEN NOT MATCHED THEN INSERT(
					 [param]
					,[value]
				)
				VALUES
				(
					 [src].[param]
					,[src].[value]
				);	 	

			END;

			
			IF SQL_VARIANT_PROPERTY(@sp,	'BaseType')	= 'nvarchar'
			BEGIN

				MERGE @context_info AS [trg] USING(
					SELECT
						 [param]	=	'sp'
						,[value]	=	CONVERT(NVARCHAR(128), @sp)				
				)	AS	[src]
				(
					 [param]
					,[value]
				)
				ON	[trg].[param]	=	[src].[param]
				WHEN MATCHED THEN UPDATE SET
					 [trg].[value] = [src].[value]
				WHEN NOT MATCHED THEN INSERT(
					 [param]
					,[value]
				)
				VALUES(
					 [src].[param]
					,[src].[value]
				);	

			END;
			

			SELECT 
				 @context_str += ISNULL([param], SPACE(0)) + '=' + ISNULL([value], SPACE(0)) + ';'
			FROM @context_info;

			SET @context_bin	=	CONVERT(VARBINARY(128), @context_str);

		END
		ELSE
		BEGIN
		
			SET @context_bin = CONVERT(VARBINARY(128), '0x', 1);

		END;
			
		SET CONTEXT_INFO @context_bin;


	END TRY
	BEGIN CATCH
	
		EXECUTE [dbo].[err_format]
			 @err_code	=	@err_code	OUTPUT
			,@err_msg	=	@err_msg	OUTPUT; 	

	END CATCH	  


	RETURN @err_code;

END;



