CREATE PROCEDURE [dbo].[rdl_data_doc_get]
	 @err_code	INT				OUTPUT
	,@err_msg	NVARCHAR(4000)	OUTPUT
	,@task_id	BIGINT
AS
BEGIN

	SET NOCOUNT ON;



	/**	
	*	Returns data to doc.rdl file
	*/
	

	SELECT	
		 @err_code	=	0
		,@err_msg	=	SPACE(0);

		
	BEGIN TRY
	
		--	static hardcodes
		DECLARE @DT_FORMAT				TINYINT	=	20;	
		DECLARE @TRN_TYPE_NORMAL		INT		=	3;
		DECLARE @STATE_ACTIVE			TINYINT	=	0;
		DECLARE @GROUPING_CRITERIA_DS1	TINYINT	=	1;
		DECLARE @GROUPING_CRITERIA_DS2	TINYINT	=	2;
		

		DECLARE @doc_id		BIGINT;
						
		
		SELECT
			 @doc_id	=	[doc_id]			
		FROM [dbo].[doc_dex]	WITH(NOLOCK)
		WHERE	[rec_id] = @task_id;		  
		
		IF @doc_id IS NULL
		BEGIN
			SET @err_code	=	70008
			SET	@err_msg	=	N'Incorrect params';			
			;THROW @err_code, @err_msg, 1;
		END;
			
			
		-- data to be used in parameters(ds0) recordset			
		DECLARE				
			 @doc_no				NVARCHAR(128)
			,@name					NVARCHAR(256)
			,@address2				NVARCHAR(128)
			,@contract_no			NVARCHAR(128)
			,@doc_dt				NVARCHAR(128)
			,@address1				NVARCHAR(128)
			,@reg_code				NVARCHAR(128)
			,@vat_code				NVARCHAR(128)
			,@ext_cust_no			NVARCHAR(128)
			,@dt_from				NVARCHAR(128)
			,@dt_to					NVARCHAR(128)
			,@pay_dt				NVARCHAR(128)
			,@doc_amount			NVARCHAR(128)
			,@contract_owner		NVARCHAR(128)				
			,@contract_id			NVARCHAR(128)
			,@contract_id_int		INT
			,@delivering_country	NVARCHAR(128)
			,@inv_no				NVARCHAR(128)
			,@invoice_type			NVARCHAR(128);
		

		SELECT 				
			 @doc_no				=	CONVERT(NVARCHAR(128), [d].[doc_no]) 
			,@name					=	CONVERT(NVARCHAR(256), [co].[name])
			,@address2				=	CONVERT(NVARCHAR(128), [co].[address2])
			,@contract_no			=	CONVERT(NVARCHAR(128), [co].[contract_no])
			,@doc_dt				=	CONVERT(NVARCHAR(128), [d].[dt_to], @DT_FORMAT)
			,@address1				=	CONVERT(NVARCHAR(128), [co].[address1])
			,@reg_code				=	CONVERT(NVARCHAR(128), [cu].[reg_code])
			,@vat_code				=	CONVERT(NVARCHAR(128), [cu].[vat_code])
			,@ext_cust_no			=	CONVERT(NVARCHAR(128), [cu].[ext_cust_no])
			,@dt_from				=	CONVERT(NVARCHAR(128), [d].[dt_from], @DT_FORMAT)
			,@dt_to					=	CONVERT(NVARCHAR(128), [d].[dt_to], @DT_FORMAT)
			,@pay_dt				=	CONVERT(NVARCHAR(128), [d].[pay_dt], @DT_FORMAT)
			,@doc_amount			=	CONVERT(NVARCHAR(128), -[d].[doc_amount])				
			,@contract_owner		=	CONVERT(NVARCHAR(128), SPACE(0))			
			,@contract_id			=	CONVERT(NVARCHAR(128), [co].[contract_id])
			,@contract_id_int		=	[co].[contract_id]
			,@delivering_country	=	CONVERT(NVARCHAR(128), SPACE(0))
			,@inv_no				=	CONVERT(NVARCHAR(128), [d].[doc_no])
			,@invoice_type			=	CONVERT(NVARCHAR(128), SPACE(0))	
		FROM [dbo].[doc]					AS	[d]		WITH(NOLOCK)
		INNER JOIN [dbo].[cred_contract]	AS	[co]	WITH(NOLOCK)
		ON		[d].[doc_id]		=	@doc_id
			AND	[d].[contract_id]	=	[co].[contract_id]
		INNER JOIN [dbo].[cred_cust]		AS	[cu]	WITH(NOLOCK)
		ON		[co].[cust_id]	=	[cu].[cust_id];	
		

		--	data to be used in recordsets ds1, ds2
		IF OBJECT_ID('tempdb..#trn') IS NOT NULL
		BEGIN
			DROP TABLE [#trn];
		END;

		;WITH [data] AS (
			SELECT 
				 [card_no]				=	[t].[card_no]																					--	VARCHAR(19)
				,[emboss2]				=	ISNULL([cd].[embos2], SPACE(0))																	--	NVARCHAR(36)
				,[loc_dt]				=	[t].[loc_dt]																					--	DATETIME
				,[ca_name]				=	COALESCE([ca].[ca_name], [ca].[ca_address], [t].[address])										--	NVARCHAR(255)	
				,[rcpt_no]				=	[t].[rcpt_no]																					--	VARCHAR(50)		
				,[item_code]			=	[t].[item_code]																					--	VARCHAR(50)
				,[item_name]			=	MAX([t].[item_name])																			--	NVARCHAR(255)
				,[item_group]			=	[t].[item_group]																				--	TINYINT	
				,[price]				=	[t].[price]																						--	DECIMAL(19, 6)
				,[quantity]				=	SUM([t].[quantity])																				--	MONEY
				,[a_rcpt_loc]			=	SUM([t].[a_rcpt_loc])																			--	BIGINT
				,[a_disc_loc]			=	SUM([t].[a_disc_loc])																			--	BIGINT			
				,[a_total_loc]			=	SUM([t].[a_total_loc])																			--	BIGINT
				,[a_total_vat_loc]		=	SUM([t].[a_total_vat_loc])																		--	BIGINT			
				,[vat_rate]				=	AVG([t].[vat_rate])																				--	SMALLMONEY		
				,[owner_id]				=	[t].[owner_id]																					--	INT
				,[is_wo_vat]			=	[t].[is_wo_vat]																					--	BIT
				,[disc_type]			=	[t].[disc_type]																					--	TINYINT		
				,[grouping_criteria]	=	CONVERT(TINYINT, IIF([t].[card_no] IS NULL, @GROUPING_CRITERIA_DS1, @GROUPING_CRITERIA_DS2))	--	TINYINT				
			FROM [dbo].[cred_trn]						AS	[t]		WITH(NOLOCK)		
			LEFT JOIN [dbo].[cred_card_det]				AS	[cd]	WITH(NOLOCK)
			ON		[t].[card_id]	=	[cd].[card_id]				
			LEFT JOIN [dbo].[cred_ca]					AS	[ca]	WITH(NOLOCK)
			ON		[t].[ca_id]	=	[ca].[ca_id]
			INNER JOIN [dbo].[cred_product_item]		AS	[pi]	WITH(NOLOCK) 
			ON		[t].[item_code]	=	[pi].[product_code]
				AND	[t].[acq_id]	=	[pi].[owner_id]
				AND	[pi].[state]	=	@STATE_ACTIVE	
			INNER JOIN [dbo].[cred_product_set_item]	AS	[psi] 	WITH(NOLOCK)			
			ON		[pi].[product_item_id]	=	[psi].[product_item_id]	
				AND	[psi].[state]			=	@STATE_ACTIVE
			WHERE	[t].[contract_id]	=	@contract_id_int	
				AND	[t].[type]			=	@TRN_TYPE_NORMAL
				AND	[t].[doc_id]		=	@doc_id
				AND	[t].[state]			=	@STATE_ACTIVE
			GROUP BY GROUPING SETS (
				(	--	ds1, invoice main page
					 [t].[vat_rate]
					,[t].[item_group]									
					,[t].[item_code]
					,[psi].[product_set_id]					
				)
				,(	--	ds2, invoice details
					 [t].[card_no]															
					,[cd].[embos2]
					,[t].[loc_dt]															
					,[ca].[ca_name]
					,[ca].[ca_address]
					,[t].[address]				
					,[t].[rcpt_no]															
					,[t].[item_code]	
					,[t].[item_group]														
					,[t].[price]																
					,[t].[quantity]															
					,[t].[a_rcpt_loc]														
					,[t].[a_disc_loc]														
					,[t].[a_total_loc]														
					,[t].[a_total_vat_loc]													
					,[t].[vat_rate]															
					,[t].[owner_id]															
					,[psi].[product_set_id]
					,[t].[disc_type]															
				)		
			)
		)		
		SELECT 
			 [card_no]				
			,[emboss2]				
			,[loc_dt]				
			,[ca_name]				
			,[rcpt_no]				
			,[item_code]			
			,[item_name]			
			,[item_group]			
			,[price]				
			,[quantity]				
			,[a_rcpt_loc]			
			,[a_disc_loc]			
			,[a_total_loc]			
			,[a_total_vat_loc]		
			,[vat_rate]				
			,[owner_id]				
			,[is_wo_vat]			
			,[disc_type]			
			,[grouping_criteria]			
		INTO [#trn] FROM [data]; 

						
		CREATE NONCLUSTERED INDEX [IX_#trn_grouping_criteria] ON [#trn]([grouping_criteria] ASC); 


		--	check data consistency
		IF (@doc_amount <> (SELECT SUM([a_total_loc]) FROM [#trn] WHERE [grouping_criteria] = @GROUPING_CRITERIA_DS1))
		BEGIN
			SET @err_code	=	70005;
			SET @err_msg	=	'System malfunction. Head and grouping by item amounts not equals @doc_id = ' + CONVERT(varchar(12), @doc_id);
			;THROW @err_code, @err_msg, 1; 
		END;

		IF (@doc_amount <> (SELECT SUM([a_total_loc]) FROM [#trn] WHERE [grouping_criteria] = @GROUPING_CRITERIA_DS2))
		BEGIN
			SET @err_code	=	70005;
			SET @err_msg	=	'System malfunction. Head and det amounts not equals @doc_id = ' + CONVERT(varchar(12), @doc_id);
			;THROW @err_code, @err_msg, 1; 
		END;
					

		--	ds1
		SELECT
			 [item_name]				=	CONVERT(NVARCHAR(128)	,	[item_name])						
			,[quantity]					=	CONVERT(DECIMAL(19, 3)	,	[quantity])												
			,[a_rcpt_loc]				=	CONVERT(DECIMAL(19, 2)	,	([a_rcpt_loc] - ([a_rcpt_loc] * [vat_rate]) / (100.0 + [vat_rate])) / 100.0)		
			,[amount_disc]				=	CONVERT(DECIMAL(19, 2)	,	(([a_rcpt_loc] - [a_total_loc]) - (([a_rcpt_loc] - [a_total_loc]) * [vat_rate]) / (100.0 + [vat_rate]))/ 100.0)
			,[avg_price_wo_vat]			=	CONVERT(DECIMAL(19, 7)	,	([a_total_loc] - [a_total_vat_loc]) / 100.0 / [quantity])
			,[amount_total_wo_vat]		=	CONVERT(DECIMAL(19, 2)	,	([a_total_loc] - [a_total_vat_loc]) / 100.0)		
			,[vat_rate]					=	CONVERT(SMALLMONEY		,	[vat_rate])
			,[vat_amount]				=	CONVERT(DECIMAL(19, 2)	,	[a_total_vat_loc] / 100.0)
			,[amount_total]				=	CONVERT(DECIMAL(19, 2)	,	[a_total_loc]  / 100.0)
			,[is_wo_vat]				=	[is_wo_vat]				
		FROM [#trn] WHERE [grouping_criteria] = @GROUPING_CRITERIA_DS1
		ORDER BY	 [a_total_loc] DESC;	
			 	
							
		--	ds2
		SELECT 
			 [card_no]				=	CONVERT(VARCHAR(19)		,	[card_no])
			,[emboss2]				=	CONVERT(NVARCHAR(36)	,	[emboss2])
			,[loc_dt]				=	CONVERT(DATETIME		,	[loc_dt])			
			,[ca_name]				=	CONVERT(NVARCHAR(128)	,	[ca_name])
			,[rcpt_no]				=	CONVERT(VARCHAR(50)		,	[rcpt_no])
			,[item_name]			=	CONVERT(NVARCHAR(255)	,	[item_name])
			,[item_group]			=	CONVERT(TINYINT			,	[item_group])
			,[price]				=	CONVERT(DECIMAL(19, 3)	,	[price])
			,[quantity]				=	CONVERT(DECIMAL(19, 3)	,	[quantity])
			,[a_rcpt_loc]			=	CONVERT(DECIMAL(19, 2)	,	[a_rcpt_loc] / 100.0)
			,[disc_per_unit]		=	CONVERT(DECIMAL(19, 4)	,	([a_rcpt_loc] - [a_total_loc]) / 100.0 / [quantity])
			,[amount_disc]			=	CONVERT(DECIMAL(19, 2)	,	([a_rcpt_loc] - [a_total_loc]) / 100.0)
			,[amount_total_wo_vat]	=	CONVERT(DECIMAL(19, 2)	,	([a_total_loc] - [a_total_vat_loc]) / 100.0)
			,[vat_rate]				=	CONVERT(SMALLMONEY		,	[vat_rate])
			,[vat_amount]			=	CONVERT(DECIMAL(19, 2)	,	[a_total_vat_loc] / 100.0)
			,[amount_total]			=	CONVERT(DECIMAL(19, 2)	,	[a_total_loc] / 100.0)			
			,[is_wo_vat]			=	CONVERT(BIT				,	[is_wo_vat])
			,[disc_type]			=	CONVERT(TINYINT			,	[disc_type])
		FROM [#trn] WHERE [grouping_criteria] = @GROUPING_CRITERIA_DS2	
		ORDER BY	 [is_wo_vat]	ASC	 
					,[item_group]	ASC
					,[card_no]		ASC
					,[loc_dt]		ASC;				
			
					
	END TRY
	BEGIN CATCH


		EXEC [dbo].[err_format]
			 @err_code	=	@err_code	OUTPUT
			,@err_msg	=	@err_msg	OUTPUT;


	END CATCH


	RETURN @err_code;


END;