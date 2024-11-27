SELECT 
    u.id AS user_id,
    u.email AS email,
    p.id AS profile_id,
    pro.id AS project_id,
    org.id AS org_id
   
FROM 
    projects_project pro  
LEFT JOIN 
    auth_user u ON pro.user_id = u.id
LEFT JOIN 
    accounts_profile p ON p.user_id = u.id
LEFT JOIN
    accounts_organization org ON org.owner = p.id
ORDER BY 
    u.id;




SELECT u.email as email, 
       u.first_name as first_name, 
       p.id as profile_id, 
       u.id as user_id ,
	   org.id as org_id
FROM accounts_profile p 
JOIN auth_user u ON p.user_id = u.id 
LEFT JOIN accounts_organization org ON org.owner = p.id;



DO $$
DECLARE
    row RECORD;
BEGIN
    FOR row IN 
        SELECT DISTINCT project_id, organization_id 
        FROM projects_projectmember
    LOOP
        UPDATE projects_project p
        SET organization_id = row.organization_id
        WHERE p.id = row.project_id;
    END LOOP;
END $$;