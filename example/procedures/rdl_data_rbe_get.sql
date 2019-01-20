CREATE PROCEDURE [dbo].[rdl_data_rbe_get]
	 @err_code	INT				OUTPUT
	,@err_msg	NVARCHAR(4000)	OUTPUT
	,@task_id	BIGINT
AS
BEGIN

	SET NOCOUNT ON;
	

	/**	
	*	Returns data to rbe.rdl file
	*/
	

	SELECT	
		 @err_code	=	0
		,@err_msg	=	SPACE(0);

		
	BEGIN TRY
	
		--	static hardcodes
		DECLARE  @DATE_STYLE					TINYINT			=	120				
				,@TRN_TYPE_NORMAL				INT				=	3
				,@STATE_ACTIVE					TINYINT			=	0
				,@BOOLEAN_TRUE					BIT				=	1
				,@DISC_TYPE_PERCENTAGE			TINYINT			=	1
				,@ZERO							DECIMAL(19, 3)	=	0.0
				,@STRING_DISC_TYPE_PERCENTAGE	NVARCHAR(64)	=	'%'
				,@STRING_DISC_TYPE_MONEY		NVARCHAR(64)	=	'EUR/ltr';	
		

		DECLARE  @doc_id		BIGINT
				,@sys_dt		DATETIME
				,@owner_id		INT
				,@sys_dt_txt	VARCHAR(19)
				,@ext_cust_no	VARCHAR(64)
				,@country_code	INT; 						

		
		SELECT
			 @doc_id	=	NULLIF([doc_id], -1)	
			,@owner_id	=	[owner_id]	 		
		FROM [dbo].[doc_dex] WITH(NOLOCK)
		WHERE	[rec_id] = @task_id;		  
		
		IF @doc_id IS NULL
		BEGIN
			SET @err_code	=	70008;
			SET @err_msg	=	N'Incorrect params(@doc_id)';			
			;THROW @err_code, @err_msg, 1;
		END;

		IF @owner_id IS NULL
		BEGIN
			SET @err_code	=	70008;
			SET @err_msg	=	N'Incorrect params(@owner_id)';			
			;THROW @err_code, @err_msg, 1;
		END;
			
		SET @sys_dt		= [dbo].[dt__get_switched](SYSDATETIMEOFFSET(), @owner_id, NULL);
		SET @sys_dt_txt = CONVERT(VARCHAR(19), @sys_dt, @DATE_STYLE);
		
		SELECT
			 @ext_cust_no = ISNULL(LTRIM(RTRIM([cu].[ext_cust_no])), SPACE(0))
		FROM [dbo].[doc]					AS	[d] WITH (NOLOCK)
		INNER JOIN [dbo].[cred_contract]	AS	[co] WITH(NOLOCK)
		ON		[d].[contract_id] = [co].[contract_id]  	
			AND	[d].[doc_id] = @doc_id 
		INNER JOIN [dbo].[cred_cust]		AS	[cu] WITH(NOLOCK)
		ON		[cu].[cust_id] = [co].[cust_id];			  
		

		SELECT
			 @country_code = [country_code]
		FROM [dbo].[cred_owner] WITH (NOLOCK)	
		WHERE		[owner_id] = @owner_id;  

			   


		IF OBJECT_ID('tempdb..#transaction_data') IS NOT NULL
		BEGIN
			DROP TABLE [#transaction_data]; 
		END;


		SELECT
			 [trn_id]				=	[t].[trn_id]	
			,[card_no] 				=	[t].[card_no] 
			,[embos2]				=	ISNULL(LTRIM(RTRIM([cd].[embos2])), SPACE(0))
			,[loc_dt]				=	[t].[loc_dt]	
			,[term_id]				=	[t].[term_id]
			,[ca_name]				=	COALESCE([ca].[ca_name], [t].[address], SPACE(0))	
			,[rcpt_no]				=	[t].[rcpt_no]
			,[item_code]			=	[t].[item_code]
			,[item_name]			=	COALESCE([t].[item_name], [pi].[name_loc], SPACE(0))
			,[item_group]			=	[t].[item_group]														
			,[price]				=	[t].[price]															
			,[quantity]				=	[t].[quantity]														
			,[a_rcpt_loc]			=	[t].[a_rcpt_loc]														
			,[a_disc_loc]			=	[t].[a_disc_loc]														
			,[disc_type]			=	[t].[disc_type]														
			,[disc_value]			=	[t].[disc_value]														
			,[vat_rate]				=	[t].[vat_rate]														
			,[a_total_vat_loc]		=	[t].[a_total_vat_loc]												
			,[a_total_loc]			=	[t].[a_total_loc]													
			,[a_total_sys]			=	[t].[a_total_sys]													
			,[curr_rate]			=	[t].[curr_rate]														
			,[curr_loc]				=	[t].[curr_loc]														
			,[acq_country_code]		=	[o].[country_code]
			,[is_local]				=	CONVERT(BIT, IIF([o].[country_code] = @country_code, @BOOLEAN_TRUE, ~@BOOLEAN_TRUE))
		INTO [#transaction_data]
		FROM [dbo].[cred_trn]				AS [t] WITH(NOLOCK)		
		INNER JOIN [dbo].[cred_card_det]	AS [cd] WITH(NOLOCK)
		ON		[cd].[card_id] = [t].[card_id]
			AND	[t].[doc_id] = @doc_id
			AND [t].[type] = @TRN_TYPE_NORMAL
			AND [t].[state] = @STATE_ACTIVE
		LEFT JOIN [dbo].[cred_product_item]	AS [pi] WITH(NOLOCK)
		ON		[pi].[product_item_id] = [t].[item_id]
		INNER JOIN [dbo].[CRED_owner]		AS [o]
		ON		[o].[owner_id] = [t].[acq_id]
		LEFT JOIN [dbo].[cred_term]			AS [tm] WITH(NOLOCK)
		ON		[tm].[term_id] = [t].[term_id]
		LEFT JOIN [dbo].[cred_ca]			AS [ca] WITH(NOLOCK)
		ON	[ca].[ca_id] = [tm].[ca_id];
		

		CREATE NONCLUSTERED INDEX [IX_#transaction_data_is_local] ON [#transaction_data]([is_local] ASC);
		CREATE NONCLUSTERED INDEX [IX_#transaction_data_loc_dt] ON [#transaction_data]([loc_dt] ASC);



		-- ds1
		SELECT 
			 [CardNo]				=	[card_no]
			,[Emboss2]				=	[embos2]
			,[LocDate]				=	CONVERT(VARCHAR(20), [loc_dt], @DATE_STYLE)
			,[FrameNo]				=	[term_id]
			,[CaName]				=	[ca_name]
			,[ReceiptNo]			=	[rcpt_no]
			,[ItemCode]				=	[item_code]
			,[ItemName]				=	[item_name]
			,[Price]				=	CONVERT(DECIMAL(19, 3), ROUND([price], 3))
			,[Quantity]				=	CONVERT(DECIMAL(19, 3), ROUND([quantity], 3))
			,[Amount]				=	CONVERT(DECIMAL(19, 2), ROUND([a_rcpt_loc] / 100.0, 2))
			,[DiscAmountPerUnit]	=	CONVERT(DECIMAL(19, 3), IIF([disc_type] = @DISC_TYPE_PERCENTAGE
											,ROUND([disc_value], 3)
											,IIF([quantity] = @ZERO, @ZERO, ([a_disc_loc] / 100.0) / [quantity])
										))		
			,[DiscountType2]		=	IIF([disc_type] = @DISC_TYPE_PERCENTAGE, @STRING_DISC_TYPE_PERCENTAGE, @STRING_DISC_TYPE_MONEY)
			,[DiscAmount]			=	CONVERT(DECIMAL(19, 2), ROUND([a_disc_loc] / 100.0, 2))
			,[AmountWoVat]			=	CONVERT(DECIMAL(19, 2), ROUND(([a_total_loc] - [a_total_vat_loc]) / 100.0, 2))
			,[VatRate]				=	CONVERT(DECIMAL(19, 2), ROUND([vat_rate], 2))
			,[VatAmount]			=	CONVERT(DECIMAL(19, 2), ROUND([a_total_vat_loc] / 100.0, 2))
			,[TotalAmount]			=	CONVERT(DECIMAL(19, 2), ROUND([a_total_loc] / 100.0, 2))
		FROM [#transaction_data]
		WHERE	[is_local] = @BOOLEAN_TRUE; 
		

		-- ds2
		SELECT
			 [Country]			=	[c].[code_a3]
			,[CurrencyLocal]	=	[cr].[curr_a]
			,[CardNo]			=	[card_no]
			,[Emboss2]			=	[embos2]
			,[LocDate]			=	CONVERT(VARCHAR(20), [loc_dt], @DATE_STYLE)
			,[CaName]			=	[ca_name]
			,[ReceiptNo]		=	[rcpt_no]
			,[ItemGroup]		=	[item_code]
			,[ItemName]			=	[item_name]
			,[Price]			=	CONVERT(DECIMAL(19, 3), ROUND([price], 3))
			,[Quantity]			=	CONVERT(DECIMAL(19, 3), ROUND([quantity], 3))
			,[LocalAmount]		=	CONVERT(DECIMAL(19, 2), ROUND([a_rcpt_loc] / 100.0, 2))
			,[LocalDiscAmount]	=	CONVERT(DECIMAL(19, 2), ROUND([a_disc_loc] / 100.0, 2))
			,[VatRate]			=	CONVERT(DECIMAL(19, 2), ROUND([vat_rate], 2))
			,[LocalVatAmount]	=	CONVERT(DECIMAL(19, 2), ROUND([a_total_vat_loc] / 100.0, 2))
			,[TotalAmountLocal]	=	CONVERT(DECIMAL(19, 2), ROUND([a_total_loc] / 100.0, 2))
			,[TotalAmountEur]	=	CONVERT(DECIMAL(19, 2), ROUND([a_total_sys] / 100.0, 2))
			,[TotalAmountSys]	=	CONVERT(DECIMAL(19, 2), ROUND([a_total_sys] / 100.0, 2))
			,[EuroRate]			=	CONVERT(DECIMAL(19, 4), [curr_rate]) 
		FROM [#transaction_data]	AS	[t]
		LEFT JOIN [dbo].[CRED_country] AS [c]
		ON		[c].[country] = [t].[acq_country_code]
		LEFT JOIN [dbo].[cred_curr] AS [cr]
		ON		[cr].[curr] = [t].[curr_loc]
		WHERE	[t].[is_local] = ~@BOOLEAN_TRUE; 
		

					
	END TRY
	BEGIN CATCH


		EXEC [dbo].[err__format]
			 @err_code	=	@err_code	OUTPUT
			,@err_msg	=	@err_msg	OUTPUT;


	END CATCH


	RETURN @err_code;


END;