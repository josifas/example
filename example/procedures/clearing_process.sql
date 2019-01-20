CREATE PROCEDURE [dbo].[clearing_process]
	 @err_code		INT				OUTPUT
	,@err_msg		NVARCHAR(4000)	OUTPUT
	,@owner_id		INT
	,@contract_id	INT
AS
BEGIN


	/**	Beta version of clearing, please make detail testing before using.
	*	Developed new queue-matching based algorithm to achieve better performance.
	*	All standard fifo algorithms are loop based (slow by design).
	*
	*	If @contract_id passed as null - performs claring of all owner contracts, 
	*	using all clearing-ready docs/payments.
	*/


	SET NOCOUNT ON;

	SELECT
		 @err_code	=	0
		,@err_msg	=	SPACE(0);
		
	BEGIN TRY


		DECLARE	 @TYPE_DOC		TINYINT =	0
				,@TYPE_PAYMENT	TINYINT =	1
				,@STATE_ACTIVE	INT		=	0
				,@AMOUNT_ZERO	BIGINT	=	0	
				,@BOOLEAN_TRUE	BIT		=	1
				,@BOOLEAN_FALSE	BIT		=	0;


		IF @@TRANCOUNT <> 1
		BEGIN
			SET @err_code = 70002;
			SET @err_msg = N'Database transaction expected';
			;THROW @err_code, @err_msg, 1;
		END;


		--	get sp name and map it with execution context
		BEGIN TRY		
			;THROW 0, '', 1;
		END TRY
		BEGIN CATCH

			DECLARE @sp NVARCHAR(128) = ERROR_PROCEDURE();
		
			EXECUTE [dbo].[context_info_set]
				 @err_code		=	@err_code	OUTPUT
				,@err_msg		=	@err_msg	OUTPUT			
				,@sp			=	@sp
				,@own			=	@owner_id			
				,@reset_context	=	0;

		END CATCH;

		IF @err_code <> 0
		BEGIN
			;THROW @err_code, @err_msg, 1;
		END;
		--	end sp name and context

		



		IF OBJECT_ID('tempdb..#clearing_data') IS NOT NULL
		BEGIN
			DROP TABLE [#clearing_data];
		END;


		DECLARE @sys_dt DATETIMEOFFSET(7) = SYSDATETIMEOFFSET();


		;WITH [debit] AS (
			SELECT
				 [contract_id]	=	[contract_id]
				,[type]			=	@TYPE_DOC
				,[id]			=	[doc_id]
				,[dt]			=	[dt_to]		
				,[amount]		=	ABS([doc_amount] + [paid_amount])
			FROM [dbo].[doc]
			WHERE	[owner_id] = @owner_id
				AND	[contract_id] = ISNULL(@contract_id, [contract_id])
				AND	[state] = @STATE_ACTIVE
				AND	[doc_amount] + [paid_amount] < @AMOUNT_ZERO

			UNION ALL

			SELECT
				 [contract_id]	=	[contract_id]
				,[type]			=	@TYPE_PAYMENT
				,[id]			=	[transfer_id]
				,[dt]			=	[transfer_dt]		
				,[amount]		=	ABS([transfer_amount])
			FROM [dbo].[transfer]
			WHERE	[owner_id] = @owner_id
				AND	[contract_id] = ISNULL(@contract_id, [contract_id])
				AND	[deleted] = @BOOLEAN_FALSE
				AND	[processed] = @BOOLEAN_TRUE
				AND	[transfer_amount] < @AMOUNT_ZERO
		)
		,[credit] AS (
			SELECT
				 [contract_id]	=	[contract_id]
				,[type]			=	@TYPE_DOC
				,[id]			=	[doc_id]
				,[dt]			=	[dt_to]		
				,[amount]		=	[doc_amount] + [paid_amount]
			FROM [dbo].[doc]
			WHERE	[owner_id] = @owner_id
				AND	[contract_id] = ISNULL(@contract_id, [contract_id])
				AND	[state] = @STATE_ACTIVE
				AND	[doc_amount] + [paid_amount] > @AMOUNT_ZERO

			UNION ALL

			SELECT
				 [contract_id]	=	[contract_id]
				,[type]			=	@TYPE_PAYMENT
				,[id]			=	[transfer_id]
				,[dt]			=	[transfer_dt]		
				,[amount]		=	[transfer_amount]
			FROM [dbo].[transfer]
			WHERE	[owner_id] = @owner_id
				AND	[contract_id] = ISNULL(@contract_id, [contract_id])
				AND	[deleted] = @BOOLEAN_FALSE
				AND	[processed] = @BOOLEAN_TRUE
				AND	[transfer_amount] > @AMOUNT_ZERO
		)	
		,[debit_queue] AS (
			SELECT		
				 [contract_id]	=	[contract_id]
				,[type]			=	[type]		
				,[id]			=	[id]		
				,[dt]			=	[dt]		
				,[amount]		=	[amount]
				,[queue_amount]	=	SUM([amount]) OVER (PARTITION BY [contract_id] ORDER BY [dt] ASC) 
			FROM [debit]				
		)
		,[credit_queue] AS (
			SELECT		
				 [contract_id]	=	[contract_id]
				,[type]			=	[type]			
				,[id]			=	[id]		
				,[dt]			=	[dt]		
				,[amount]		=	[amount]
				,[queue_amount]	=	SUM([amount]) OVER (PARTITION BY [contract_id] ORDER BY [dt] ASC) 
			FROM [credit]
		)
		SELECT
			 [rec_id]			=	IDENTITY(INT, 1, 1)
			,[contract_id]		=	ISNULL([dq].[contract_id], [cq].[contract_id])	 
			,[debit_type]		=	[dq].[type]
			,[debit_id]			=	[dq].[id]
			,[debit_dt]			=	[dq].[dt]
			,[debit_amont]		=	[dq].[amount]
			,[debit_left]		=	ABS(IIF(SIGN(-[dq].[queue_amount] + [cq].[queue_amount]) = -1	, -[dq].[queue_amount] + [cq].[queue_amount], @AMOUNT_ZERO))
			,[credit_type]		=	[cq].[type]
			,[credit_id]		=	[cq].[id]
			,[credit_dt]		=	[cq].[dt]
			,[credit_amount]	=	[cq].[amount]
			,[credit_left]		=	ABS(IIF(SIGN(-[dq].[queue_amount] + [cq].[queue_amount]) = 1	, -[dq].[queue_amount] + [cq].[queue_amount], @AMOUNT_ZERO))
			,[clearing_balance] =	-[dq].[queue_amount] + [cq].[queue_amount] 
			,[clearing_date]	=	ISNULL([cq].[dt], [dq].[dt])
		INTO [#clearing_data]		
		FROM [debit_queue]			AS	[dq]
		INNER JOIN [credit_queue]	AS	[cq]
		ON		[dq].[contract_id] = [cq].[contract_id]
			AND	([dq].[queue_amount] > ([cq].[queue_amount] - [cq].[amount]))
			AND	([dq].[queue_amount] - [dq].[amount]) < [cq].[queue_amount]
		ORDER BY	 ISNULL([dq].[contract_id], [cq].[contract_id])	ASC
					,ISNULL([cq].[dt], [dq].[dt]) ASC;

			 
		CREATE CLUSTERED INDEX [PK_#clearing_data] ON [#clearing_data]([rec_id]); 
		

		INSERT INTO [dbo].[doc_clearing](
			 [contract_id]	
			,[debit_id]		
			,[debit_source]	
			,[credit_id]	
			,[credit_source]
			,[amount]		
			,[sys_dt]		
			,[state]		
		)
		SELECT	
			 [contract_id]		=	[contract_id]
			,[debit_id]			=	[debit_id]	
			,[debit_source]		=	[debit_type]
			,[credit_id]		=	[credit_id]
			,[credit_source]	=	[credit_type]
			,[amount]			=	[credit_amount] - [credit_left]
			,[sys_dt]			=	@sys_dt		
			,[state]			=	@STATE_ACTIVE
		FROM [#clearing_data];


		CREATE NONCLUSTERED INDEX [IX_#clearing_data_credit_type] ON [dbo].[#clearing_data]([credit_type]) INCLUDE ([contract_id], [credit_id], [credit_dt], [credit_left]);
		CREATE NONCLUSTERED INDEX [IX_#clearing_data_debit_type] ON [dbo].[#clearing_data]([debit_type]) INCLUDE ([contract_id], [debit_id], [debit_dt], [debit_left]);
		CREATE NONCLUSTERED INDEX [IX_#clearing_data_combined] ON [dbo].[#clearing_data]([contract_id], [credit_type], [credit_id]) INCLUDE ([credit_dt], [credit_left]);


		/**	debit type @TYPE_DOC, credit type doesn't matters */
		UPDATE [dbo].[doc]	SET
			 [paid_amount]		=	-[d].[doc_amount] - [c].[debit_left]
			,[paid_dt]			=	[c].[paid_dt]
			,[paid]				=	[c].[paid]
			,[is_synchronized]	=	@BOOLEAN_FALSE
		FROM [dbo].[doc]	AS	[d]
		INNER JOIN (
			SELECT
				 [doc_id]		=	[debit_id]
				,[contract_id]	=	[contract_id]
				,[debit_left]	=	MIN([debit_left])
				,[paid_dt]		=	IIF(CONVERT(BIT, MIN([debit_left])) = @BOOLEAN_FALSE, MAX([credit_dt]), NULL)
				,[paid]			=	~CONVERT(BIT, MIN([debit_left]))
			FROM [#clearing_data] 
			WHERE		[debit_type] = @TYPE_DOC 
			GROUP BY	 [contract_id]
						,[debit_id]
						,[debit_type]
		)				AS	[c]
		ON		[d].[contract_id] = [c].[contract_id]
			AND	[d].[doc_id] = [c].[doc_id];


		/**	debit type @TYPE_PAYMENT, credit type doesn't matters */
		UPDATE [dbo].[transfer]	SET
			 [clearing_amount]	=	- [c].[debit_left]	
		FROM [dbo].[transfer]	AS	[t]
		INNER JOIN (
			SELECT
				 [transfer_id]	=	[debit_id]
				,[contract_id]	=	[contract_id]
				,[debit_left]	=	MIN([debit_left])		
			FROM [#clearing_data] 
			WHERE		[debit_type] = @TYPE_PAYMENT 
			GROUP BY	 [contract_id]
						,[debit_id]
						,[debit_type]
		)				AS	[c]
		ON		[t].[contract_id] = [c].[contract_id]
			AND	[t].[transfer_id] = [c].[transfer_id];


		/**	debit type doesn't matters, credit type @TYPE_PAYMENT */
		UPDATE [dbo].[transfer]	SET
			 [clearing_amount]	=	[c].[credit_left]	
		FROM [dbo].[transfer]	AS	[t]
		INNER JOIN (
			SELECT
				 [transfer_id]	=	[credit_id]
				,[contract_id]	=	[contract_id]
				,[credit_left]	=	MIN([credit_left])		
			FROM [#clearing_data] 
			WHERE		[credit_type] = @TYPE_PAYMENT 
			GROUP BY	 [contract_id]
						,[credit_id]
						,[credit_type]
		)				AS	[c]
		ON		[t].[contract_id] = [c].[contract_id]
			AND	[t].[transfer_id] = [c].[transfer_id];


		/**	debit type doesn't matters, credit type @TYPE_DOC */
		UPDATE [dbo].[doc]	SET
			 [paid_amount]		=	[d].[doc_amount] - [c].[credit_left]
			,[paid_dt]			=	[c].[paid_dt]
			,[paid]				=	[c].[paid]
			,[is_synchronized]	=	@BOOLEAN_FALSE
		FROM [dbo].[doc]	AS	[d]
		INNER JOIN (
			SELECT
				 [doc_id]		=	[credit_id]
				,[contract_id]	=	[contract_id]
				,[credit_left]	=	MIN([credit_left])
				,[paid_dt]		=	IIF(CONVERT(BIT, MIN([credit_left])) = @BOOLEAN_FALSE, MAX([credit_dt]), NULL)
				,[paid]			=	~CONVERT(BIT, MIN([credit_left]))
			FROM [#clearing_data] 
			WHERE		[credit_type] = @TYPE_DOC 
			GROUP BY	 [contract_id]
						,[credit_id]
						,[credit_type]
		)				AS	[c]
		ON		[d].[contract_id] = [c].[contract_id]
			AND	[d].[doc_id] = [c].[doc_id];
			

	END TRY		 
	BEGIN CATCH
		
		EXECUTE [dbo].[err_format]
			 @err_code	=	@err_code	OUTPUT
			,@err_msg	=	@err_msg	OUTPUT;
				
	END CATCH;


	RETURN @err_code;
	
END
