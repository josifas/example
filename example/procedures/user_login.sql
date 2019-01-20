CREATE PROCEDURE [dbo].[user_login]
	 @err_code				INT				OUTPUT
	,@err_msg				NVARCHAR(4000)	OUTPUT
	,@login					NVARCHAR(128)
	,@pass_hash				VARBINARY(32)
	,@add_data				NVARCHAR(4000)	
	,@tz_offset				INT
	,@language_id			INT
	,@ip					VARCHAR(15)
	,@owner_id				INT			
	,@sid					BINARY(32)		OUTPUT
	,@user_id				INT				OUTPUT	
	,@group_id				INT				OUTPUT	
	,@frontend_version		VARCHAR(20)		OUTPUT
	,@force_pass_change		BIT				OUTPUT
	,@login_attempts_left	TINYINT			OUTPUT
	,@rv					BINARY(8)		OUTPUT
	,@frontend_type			TINYINT	
AS
BEGIN

	SET NOCOUNT ON;

	SELECT
		 @err_code	=	0
		,@err_msg	=	SPACE(0);  


	BEGIN TRY
	
		IF @@TRANCOUNT <> 0
		BEGIN
			SET @err_code	=	70002;
			SET @err_msg	=	N'Invalid db transaction context';
			;THROW @err_code, @err_msg, 1;
		END;


		--	clear output params
		SELECT
			 @sid					=	NULL 
			,@user_id				=	NULL			
			,@group_id				=	NULL
			,@force_pass_change		=	0
			,@login_attempts_left	=	NULL;
						


		--	check params		
		IF @owner_id IS NULL	
		BEGIN
			SET @err_code	=	70008;
			SET @err_msg	=	'Incorrect params(@owner_id)';
			;THROW @err_code, @err_msg, 1;
		END; 


		IF @login IS NULL	
		BEGIN
			SET @err_code	=	70008;
			SET @err_msg	=	'Incorrect params(@login)';
			;THROW @err_code, @err_msg, 1;
		END; 
			
		IF @pass_hash IS NULL			
		BEGIN
			SET @err_code	=	70008;
			SET @err_msg	=	'Incorrect params(@pass_hash)';
			;THROW @err_code, @err_msg, 1;
		END; 

		IF @tz_offset IS NULL			
		BEGIN
			SET @err_code	=	70008;
			SET @err_msg	=	'Incorrect params(@tz_offset)';
			;THROW @err_code, @err_msg, 1;
		END; 

		IF @language_id IS NULL		
		BEGIN
			SET @err_code	=	70008;
			SET @err_msg	=	'Incorrect params(@language_id)';
			;THROW @err_code, @err_msg, 1;
		END; 

		IF @ip IS NULL
		BEGIN
			SET @err_code	=	70008;
			SET @err_msg	=	'Incorrect params(@ip)';
			;THROW @err_code, @err_msg, 1;
		END; 
					
		IF @tz_offset NOT BETWEEN -1440 AND 1440
		BEGIN
			SET @err_code	= 70008;
			SET @err_msg	= N'Invalid parameters(@tz_offset)';
			;THROW @err_code, @err_msg, 1;
		END;
		--	end check params


		DECLARE @backend_version		VARCHAR(20);
		DECLARE @session_timeout		INT;						
		DECLARE @sys_dt					DATETIMEOFFSET(7)	=	SYSDATETIMEOFFSET();
		

		DECLARE @out TABLE (
			 [rv]	BINARY(8)
		);

		
		BEGIN TRAN

		--	try to login
		UPDATE 
			[dbo].[frontend_user]			
		SET			
			 @user_id					=	[fu].[user_id]			
			,@group_id					=	[fu].[group_id]
			,@backend_version			=	[ft].[backend_version]	
			,@session_timeout			=	[fto].[session_timeout]		
			,@force_pass_change			=	[fu].[force_pass_change]			
			,[is_active]				=	1
			,[last_login_dt]			=	@sys_dt
			,[failed_login_count]		=	0
			,[first_failed_login_dt]	=	NULL
			,[locked_dt]				=	NULL
		OUTPUT
			 [inserted].[row_version]
		INTO @out (
			 [rv]
		)
		FROM [dbo].[frontend_user]				AS	[fu]
		INNER JOIN [dbo].[frontend_user_group]	AS	[fug]
		ON		[fu].[login]		=	@login
			AND	[fu].[owner_id]		=	@owner_id
			AND	[fug].[owner_id]	=	@owner_id
			AND	[fu].[pass_hash]	=	@pass_hash
			AND	[fu].[group_id]		=	[fug].[group_id]			
			AND	[fu].[state]		=	0
			AND	[fug].[state]		=	0
		INNER JOIN [dbo].[frontend_type]		AS	[ft]
		ON		[fug].[frontend_type]	=	[ft].[frontend_type]
			AND	[ft].[frontend_type]	=	ISNULL(@frontend_type, [ft].[frontend_type])
		INNER JOIN [dbo].[frontend_type_owner]	AS	[fto]
		ON		[ft].[frontend_type]	=	[fto].[frontend_type]				
			AND	[fto].[owner_id]		=	[fu].[owner_id]
		WHERE	[fu].[is_active] = 1
			OR	(		[fu].[is_active] = 0
					AND	DATEDIFF(MI, ISNULL([fu].[locked_dt], [fu].[first_failed_login_dt]), @sys_dt) > [fto].[unlock_after]	
				);

		IF @@ROWCOUNT = 1 AND @user_id IS NOT NULL
		BEGIN

			--	login successfull			

			IF @frontend_version <> @backend_version
			BEGIN
				SET @frontend_version	= @backend_version;
				SET @err_code			= 70028;
				SET @err_msg			= N'Frontend version is not valid';
				;THROW @err_code, @err_msg, 1;
			END
			ELSE 
			BEGIN
				SET @frontend_version = @backend_version;
			END;


			SELECT TOP 1 @rv = [rv] FROM @out ORDER BY 1 DESC;
			

			EXECUTE [dbo].[frontend_session__insert] 
				 @err_code		=	@err_code			OUTPUT
				,@err_msg		=	@err_msg			OUTPUT
				,@sid			=	@sid				OUTPUT			
				,@user_id		=	@user_id				
				,@login_dt		=	@sys_dt				
				,@timeout		=	@session_timeout				
				,@owner_id		=	@owner_id				
				,@tz_offset		=	@tz_offset				
				,@language_id	=	@language_id			
				,@ip			=	@ip					
				,@add_data		=	@add_data				
				
			IF @err_code	<>	0
			BEGIN
				;THROW @err_code, @err_msg, 1;
			END;					
			
			
			COMMIT

			/**	Unfortunately, we need make some delay on successfull logins too. Its bad practice to return success result to 
			*	frontend immediatelly. Because most hackers makes prediction of bad pass/login and kills session after 
			*	few miliseconds of waiting without response. So, offer them to wait something randomly here (0-1s)			
			*/			
			DECLARE @delay_ok VARCHAR(256) =  '00:00:00.' + RIGHT('000' + CONVERT(VARCHAR(3), ROUND(RAND() * 1000.0 - 1, 0)), 3);
			WAITFOR DELAY @delay_ok;

		END
		ELSE
		BEGIN
			
			--	login failed
										
				
			;WITH [user_state] AS (
				--	recalc user state before using it in futher logic
				SELECT
					 [user_id]					
					,[is_active]				
					,[failed_login_count]		
					,[first_failed_login_dt]	
					,[locked_dt]				
					,[max_failed_login_count]
				FROM [dbo].[frontend_user_refreshed_state_get](
						 NULL		-- @user_id	
						,@login		
						,@owner_id
						,NULL	
					)					
				WHERE	[frontend_type] =	ISNULL(@frontend_type, [frontend_type])
					AND	[state]			=	0											 			
					AND	[state_group]	=	0
			)		
			--	then update user state (failed attempt related fields)			
			UPDATE 
				[dbo].[frontend_user]
			SET								
				 [is_active]				=	IIF([us].[max_failed_login_count] <= ([us].[failed_login_count] + 1), 0, [us].[is_active])				
				,[failed_login_count]		=	IIF([us].[max_failed_login_count] > [us].[failed_login_count], [us].[failed_login_count] + 1, [fu].[failed_login_count]) 
				,[first_failed_login_dt]	=	IIF([us].[first_failed_login_dt] IS NOT NULL, [us].[first_failed_login_dt], @sys_dt)
				,[locked_dt]				=	IIF([us].[max_failed_login_count] <= ([us].[failed_login_count] + 1) AND [us].[locked_dt] IS NULL, @sys_dt, [us].[locked_dt])	
				,@login_attempts_left		=	[us].[max_failed_login_count] - (IIF(
													 [us].[max_failed_login_count] > [us].[failed_login_count]
													,[us].[failed_login_count] + 1
													,[us].[failed_login_count]
												))
			FROM [dbo].[frontend_user]		AS	[fu]
			INNER JOIN [user_state]			AS	[us]
			ON		[fu].[user_id]		=	[us].[user_id];		
								
							
											
			COMMIT
						
			/**
			*	We do not know anything about frontend type here. And, it does not matters. 
			*	We cannot have diff delay for attempts with existing logins, and not existing. 
			*	To avoid brutreforce, waitfor 2-3s randomly after login failed. Same for all frontend types.
			*/			
			DECLARE @delay_failed VARCHAR(256) =  '00:00:02.' + RIGHT('000' + CONVERT(VARCHAR(3), ROUND(RAND() * 1000.0 - 1, 0)), 3);
			WAITFOR DELAY @delay_failed;		
			

			SET @err_code	=	IIF(@login_attempts_left = 0,  70031/*account locked*/, 70023/*invalid login*/);
			SET @err_msg	=	N'Invalid login or psw';
			;THROW @err_code, @err_msg, 1;

			--	end failed login logic
		END;
					

	END TRY
	BEGIN CATCH

		IF XACT_STATE() <> 0
		BEGIN
			ROLLBACK;
		END;

		EXEC [dbo].[err_format]
			 @err_code	=	@err_code	OUTPUT
			,@err_msg	=	@err_msg	OUTPUT;

	END CATCH
		

	RETURN @err_code;

END;
	

	


