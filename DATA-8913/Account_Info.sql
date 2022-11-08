SELECT DISTINCT u.id,
    u.first_name,
    u.last_name,
    DATE(u.birthdate) AS birthdate,
    e.email,
    INITCAP(a.line_1) as line_1,
    a.line_2,
    INITCAP(a.city) as City,
    a.region,
    a.postal_code,
    p.number as phone_number,
    k.document_type,
    --k.document_region_code,
    k.document_expiration_date,
    k.document_reference_number,
    MIN(DATE(u.inserted_at)) as account_created,
    MAX(DATE(u.last_login_at)) as last_log_in
    FROM Identity_db_history_pii.users  u
    LEFT JOIN Identity_db_history_pii.kyc_verifications k ON u.id = k.user_id
    LEFT JOIN Identity_db_history_pii.emails e ON u.id = e.user_id
    LEFT JOIN Identity_db_history_pii.addresses a ON u.id = a.user_id 
    LEFT JOIN Identity_db_history_pii.phone_numbers p ON u.id = p.user_id
    WHERE k.status IN ('success','approved')
        AND k.document_type IS NOT NULL
        AND u.registration_completed_at IS NOT NULL
        AND u.id IN ('da57358c-a075-4abb-b8d5-1980355900bb',
                    '497eea56-00e1-47c7-a5fb-eac737ab6c64',
                    '78745faf-9408-4126-834e-5753b2758d5d',
                    '510655a3-7d3b-4aae-a7f1-0649ef8b0673',
                    'e17f6601-8985-4709-9c1e-c7ed23cb751f',
                    '61f4aa6a-5b8e-4a33-8067-1b44d42302b0',
                    'e5d24faf-6898-49ad-b05d-16efbbd75e94',
                    '2c0f3671-d0b9-4fe2-b0d3-47d4867d2db8',
                    '473445f6-2395-4208-9f1e-cefa28b8d47c',
                    '4a97c2c6-ea01-4df8-9a0e-0b0672c7a0c2',
                    '5a112216-8ee1-44d0-aa6b-fa44c36718ae',
                    '82058583-3afc-4e23-89e2-396e0bc28bab',
                    'af84d578-6e88-4fe3-95e3-ad30f9a337d4',
                    'fc091a18-d6ac-4086-86bf-5b3dd20dcff7',
                    '298a094c-08eb-48d8-a564-e4f1678c8306',
                    'a9f3344e-a446-4016-919f-1b9ba715a3d8',
                    '1ed6b82d-7d0b-49a2-8786-43fa5aefdaa9')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14;



