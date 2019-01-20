CREATE FUNCTION [dbo].[frontend_related_user_list](
	 @user_id	INT
)
RETURNS TABLE
AS

/**	Returns users list of customer scope
*/

RETURN (
	WITH [usr] AS
	(
		SELECT
			 [user_id]					=	ISNULL([parent_user_id], [user_id])
			,[max_data_access_level]	=	[max_data_access_level]	
		FROM [dbo].[frontend_user]
		WHERE	[user_id] =  @user_id
	)
	,[down_list] AS
	(
		SELECT
			 [user_id]					=	[fu].[user_id]
			,[parent_user_id]			=	[fu].[parent_user_id]
			,[max_data_access_level]	=	[fu].[max_data_access_level]
		FROM [dbo].[frontend_user]	AS	[fu]
		INNER JOIN [usr]			AS	[u]
		ON		[fu].[user_id] = [u].[user_id]
		
		UNION ALL
	
		SELECT
			 [user_id]					=	[t1].[user_id]			
			,[parent_user_id]			=	[t1].[parent_user_id]
			,[max_data_access_level]	=	[t1].[max_data_access_level]
		FROM [dbo].[frontend_user]	AS	[t1]
		INNER JOIN [down_list]		AS	[t2]
		ON		[t1].[parent_user_id] = [t2].[user_id]		
	) 
	,[up_list] AS
	(
		SELECT
			 [user_id]					=	[fu].[user_id]
			,[parent_user_id]			=	[fu].[parent_user_id]
			,[max_data_access_level]	=	[fu].[max_data_access_level]
		FROM	[dbo].[frontend_user]	AS	[fu]
		INNER JOIN [usr]				AS	[u]
		ON		[fu].[user_id] = [u].[user_id]
		
		UNION ALL
	
		SELECT
			 [user_id]					=	[t1].[user_id]			
			,[parent_user_id]			=	[t1].[parent_user_id]
			,[max_data_access_level]	=	[t1].[max_data_access_level]
		FROM [dbo].[frontend_user]	AS	[t1]
		INNER JOIN [up_list]		AS	[t2]
		ON		[t1].[user_id] = [t2].[parent_user_id]		
	) 
	,[final_list] AS
	(
		SELECT
			 [user_id]
			,[parent_user_id]	
			,[max_data_access_level]
		FROM [down_list]

		UNION ALL

		SELECT
			 [user_id]
			,[parent_user_id]	
			,[max_data_access_level]
		FROM [up_list]
	)
	SELECT
		 [user_id] = [fl].[user_id]
	FROM [final_list]	AS	[fl]
	CROSS JOIN [usr]	AS	[u]
	GROUP BY	 [fl].[user_id]
	 
) 		
					 