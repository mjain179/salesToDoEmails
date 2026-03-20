from config import *
import dotenv
import os
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import psycopg2
import pandas as pd
from datetime import datetime

dotenv.load_dotenv()

def createConnection():
    params = game_db_config()
    gameDbConn = psycopg2.connect(**params)
    gameDbCur = gameDbConn.cursor()
    return gameDbConn, gameDbCur

def get_mail_credentials():
    with open('mail_credentials.json') as json_file:
        data = json.load(json_file)
        return data['email'], data['password']

def getPatientsForSalespersonReport(dbCursor):
    query = '''
    WITH ins_age_calculations AS (
        SELECT 
            sf_ins.story_id AS destination,
            sf_ins.age_id,
            sf_ins.status,
            sf_ins.destination AS patient_contact_id,
            ins.latest_insurance_update,
            ins.earliest_insurance_update,
            i.prescription,
            i.medical_records,
            i.primary_insurance_card,
            i.delivery_ticket,
            c.first_name AS patient_first_name,
            c.last_name AS patient_last_name,
            c5.contact_id AS salesperson_contact_id,
            c5.email AS salesperson_email,
            c5.first_name AS salesperson_first_name,
            c2.first_name AS doctor_first_name,
            c2.last_name AS doctor_last_name,
            c2.phone_number AS doctor_phone,
            c2.doc_fax AS doctor_fax,
            c2.email AS doctor_email,
            s_confirm.story_id AS confirm_fax_id,
            s_confirm.status AS confirm_fax_status,
            c_mrs.contact_id AS mrs_contact_id,
            c_mrs.first_name AS mrs_first_name,
            c_mrs.last_name AS mrs_last_name,
            c_clinic.first_name AS clinic_name,
            NULL::float AS stuck_age_in_days,
            EXTRACT(EPOCH FROM (NOW() - ins.earliest_insurance_update)) / 86400 AS insurance_age_in_days,
            EXTRACT(EPOCH FROM (NOW() - ins.latest_insurance_update)) / 86400 AS insurance_status_age_in_days
        FROM story_fresh AS sf_ins
        LEFT JOIN insurance_fresh AS i
            ON i.insurance_id = sf_ins.story_id
        LEFT JOIN contacts_fresh AS c
            ON c.contact_id = sf_ins.destination
        LEFT JOIN (
            SELECT 
                story_id,
                MAX(created_at) AS latest_insurance_update,
                MIN(created_at) AS earliest_insurance_update
            FROM story
            WHERE type = 'insurance'
                AND status IS NOT NULL
                AND status <> 'duplicate'
            GROUP BY story_id
        ) ins
            ON ins.story_id = sf_ins.story_id
        LEFT JOIN story_fresh AS s3
            ON s3.type = 'clinicPatient'
            AND s3.destination = sf_ins.destination
        LEFT JOIN contacts_fresh AS c_clinic
            ON c_clinic.contact_id = s3.origin
        LEFT JOIN LATERAL (
            SELECT s4.origin, s4.destination
            FROM story_fresh AS s4
            WHERE s4.type = 'inservice'
                AND s4.destination = s3.origin
            ORDER BY s4.created_at DESC
            LIMIT 1
        ) s4_latest ON true
        LEFT JOIN contacts_fresh AS c5
            ON c5.contact_id = s4_latest.origin
        LEFT JOIN story_fresh AS s2 
            ON s2.type = 'prescriberFax' 
            AND sf_ins.destination = s2.destination
        LEFT JOIN contacts_fresh AS c2 
            ON s2.origin = c2.contact_id
        LEFT JOIN story_fresh AS s_confirm
            ON s_confirm.type = 'confirmFax'
            AND s_confirm.origin = sf_ins.story_id
            AND s_confirm.status = 'Confirmed'
        LEFT JOIN story_fresh AS s_claims
            ON s_claims.type = 'claimsDocs'
            AND s_claims.destination = sf_ins.story_id
        LEFT JOIN contacts_fresh AS c_mrs
            ON c_mrs.contact_id = s_claims.origin
        WHERE sf_ins.type = 'insurance'
            AND ins.earliest_insurance_update >= '2025-06-01'
            AND c5.email LIKE '%motusnova%'
            AND c5.contact_id=408182
    ),

    stuck_age_calculations AS (
        SELECT 
            s.destination,
            s.age_id,
            sf_ins.status,
            sf_ins.destination AS patient_contact_id,
            ins.latest_insurance_update,
            ins.earliest_insurance_update,
            i.prescription,
            i.medical_records,
            i.primary_insurance_card,
            i.delivery_ticket,
            c.first_name AS patient_first_name,
            c.last_name AS patient_last_name,
            c5.contact_id AS salesperson_contact_id,
            c5.email AS salesperson_email,
            c5.first_name AS salesperson_first_name,
            c2.first_name AS doctor_first_name,
            c2.last_name AS doctor_last_name,
            c2.phone_number AS doctor_phone,
            c2.doc_fax AS doctor_fax,
            c2.email AS doctor_email,
            s_confirm.story_id AS confirm_fax_id,
            s_confirm.status AS confirm_fax_status,
            c_mrs.contact_id AS mrs_contact_id,
            c_mrs.first_name AS mrs_first_name,
            c_mrs.last_name AS mrs_last_name,
            c_clinic.first_name AS clinic_name,
            CASE 
                WHEN s.age_id IS NULL THEN 
                    EXTRACT(EPOCH FROM (NOW() - stuck.earliest_stuck_update)) / 86400
                ELSE 
                    EXTRACT(EPOCH FROM (NOW() - a.begin_time)) / 86400
            END AS stuck_age_in_days,
            CASE 
                WHEN s2.age_id IS NULL THEN 
                    EXTRACT(EPOCH FROM (NOW() - ins.earliest_insurance_update)) / 86400
                ELSE 
                    EXTRACT(EPOCH FROM (NOW() - a2.begin_time)) / 86400
            END AS insurance_age_in_days,
            CASE 
                WHEN s2.age_id IS NULL THEN 
                    EXTRACT(EPOCH FROM (NOW() - ins.latest_insurance_update)) / 86400
                ELSE 
                    EXTRACT(EPOCH FROM (NOW() - a2.begin_time)) / 86400
            END AS insurance_status_age_in_days
        FROM story_fresh AS s
        LEFT JOIN insurance_fresh AS i
            ON i.insurance_id = s.destination
        LEFT JOIN story_fresh sf_ins
            ON s.destination = sf_ins.story_id AND sf_ins.type = 'insurance'
        LEFT JOIN contacts_fresh AS c
            ON c.contact_id = sf_ins.destination
        LEFT JOIN age_table AS a
            ON a.age_id = s.age_id
        LEFT JOIN (
            SELECT 
                story_id,
                MAX(created_at) AS latest_insurance_update,
                MIN(created_at) AS earliest_insurance_update
            FROM story
            WHERE type = 'insurance'
                AND status IS NOT NULL
                AND status <> 'duplicate'
            GROUP BY story_id
        ) ins
            ON ins.story_id = s.destination
        LEFT JOIN (
            SELECT 
                story_id,
                MAX(created_at) AS latest_stuck_update,
                MIN(created_at) AS earliest_stuck_update
            FROM story
            WHERE type = 'stuck'
                AND status IS NOT NULL
                AND status NOT IN ('duplicate', 'complete')
            GROUP BY story_id
        ) stuck
            ON stuck.story_id = s.story_id
        LEFT JOIN story_fresh AS s3
            ON s3.type = 'clinicPatient'
            AND s3.destination = sf_ins.destination
        LEFT JOIN contacts_fresh AS c_clinic
            ON c_clinic.contact_id = s3.origin
        LEFT JOIN LATERAL (
            SELECT s4.origin, s4.destination
            FROM story_fresh AS s4
            WHERE s4.type = 'inservice'
                AND s4.destination = s3.origin
            ORDER BY s4.created_at DESC
            LIMIT 1
        ) s4_latest ON true
        LEFT JOIN contacts_fresh AS c5
            ON c5.contact_id = s4_latest.origin
        LEFT JOIN story_fresh AS s2 
            ON s2.type = 'prescriberFax' 
            AND sf_ins.destination = s2.destination
        LEFT JOIN contacts_fresh AS c2 
            ON s2.origin = c2.contact_id
        LEFT JOIN story_fresh AS s_confirm
            ON s_confirm.type = 'confirmFax'
            AND s_confirm.origin = s.destination
            AND s_confirm.status = 'Confirmed'
        LEFT JOIN story_fresh AS s_claims
            ON s_claims.type = 'claimsDocs'
            AND s_claims.destination = s.destination
        LEFT JOIN contacts_fresh AS c_mrs
            ON c_mrs.contact_id = s_claims.origin
        LEFT JOIN story AS s_ins
            ON s_ins.story_id = s.destination
        LEFT JOIN age_table a2
            ON a2.age_id = s_ins.age_id
        WHERE s.type = 'stuck'
            AND ins.earliest_insurance_update >= '2025-06-01'
            AND c5.email LIKE '%motusnova%'
            AND c5.contact_id=408182
    )

    SELECT *
    FROM (
        SELECT DISTINCT ON (destination)
            destination, age_id, status, patient_contact_id,
            patient_first_name, patient_last_name, earliest_insurance_update,
            stuck_age_in_days, insurance_age_in_days, insurance_status_age_in_days,
            prescription, medical_records, primary_insurance_card, delivery_ticket,
            salesperson_contact_id, salesperson_email, salesperson_first_name,
            doctor_first_name, doctor_last_name, doctor_phone, doctor_fax, doctor_email,
            confirm_fax_id, confirm_fax_status, mrs_contact_id, mrs_first_name, mrs_last_name, clinic_name
        FROM ins_age_calculations
        WHERE
        (
            (status = 'needPrescriptionAndMedicalRecords' 
                AND insurance_status_age_in_days > 21 AND insurance_status_age_in_days <= 90
                AND prescription IS NULL AND medical_records IS NULL)
            OR
            (status = 'needPrescriptionOnly'
                AND insurance_status_age_in_days > 21 AND insurance_status_age_in_days <= 90
                AND prescription IS NULL)
            OR
            (status = 'needMedicalRecordsOnly'
                AND insurance_status_age_in_days > 21 AND insurance_status_age_in_days <= 90
                AND medical_records IS NULL)
        )
        AND insurance_age_in_days <= 90
        ORDER BY destination, earliest_insurance_update ASC
    ) p1

    UNION

    SELECT *
    FROM (
        SELECT DISTINCT ON (destination)
            destination, age_id, status, patient_contact_id,
            patient_first_name, patient_last_name, earliest_insurance_update,
            stuck_age_in_days, insurance_age_in_days, insurance_status_age_in_days,
            prescription, medical_records, primary_insurance_card, delivery_ticket,
            salesperson_contact_id, salesperson_email, salesperson_first_name,
            doctor_first_name, doctor_last_name, doctor_phone, doctor_fax, doctor_email,
            confirm_fax_id, confirm_fax_status, mrs_contact_id, mrs_first_name, mrs_last_name, clinic_name
        FROM stuck_age_calculations
        WHERE
        (
            (status IN ('insuranceCard', 'insuranceTerminated', 'insuranceVerification', 'needDoctor', 'requestedInfoAdded', 'missingPatientInfo','missingProductInfo')
                AND stuck_age_in_days > 4 AND stuck_age_in_days <= 90)
            OR
            (status IN ('reject','rejectAuth','close','needDME','notCovered','info','readyToBill')
                AND stuck_age_in_days > 3 AND stuck_age_in_days <= 90)
            OR
            (status IN ('auth','telehealth','HMO')
                AND stuck_age_in_days > 7 AND stuck_age_in_days <= 90)
            OR
            (status = 'deliveryTicket'
                AND stuck_age_in_days > 5 AND stuck_age_in_days <= 90
                AND delivery_ticket IS NOT NULL)
            OR
            (status = 'submitted'
                AND stuck_age_in_days > 30 AND stuck_age_in_days <= 90)
            OR
            (status = 'newClaim'
                AND stuck_age_in_days > 2 AND stuck_age_in_days <= 90)
            OR 
            (status = 'appeal'
                AND stuck_age_in_days > 15 AND stuck_age_in_days <= 90)
        )
        AND insurance_age_in_days <= 90
        ORDER BY destination, earliest_insurance_update ASC
    ) p2
    '''

    dbCursor.execute(query)
    columnNames = [desc[0] for desc in dbCursor.description]
    rows = dbCursor.fetchall()
    df = pd.DataFrame(rows, columns=columnNames)
    return df

def getPatientsWithIncompleteSignUpReport(dbCursor):
    query = '''
    WITH sales_story_age AS (
        SELECT
            s.story_id,
            s.destination AS patient_contact_id,
            s.status,
            s.created_at AS sales_story_created_at,
            c5.contact_id AS salesperson_contact_id,
            c5.email AS salesperson_email,
            c5.first_name AS salesperson_first_name,
            c.first_name AS patient_first_name,
            c.last_name AS patient_last_name,
            c3.first_name AS clinic_name,
            EXTRACT(EPOCH FROM (NOW() - s.created_at)) / 86400 AS age_in_days
        FROM story_fresh AS s
        LEFT JOIN contacts_fresh AS c
            ON c.contact_id = s.destination
        LEFT JOIN story_fresh AS s2
            ON s2.type = 'clinicPatient'
            AND s2.destination = s.destination
        LEFT JOIN contacts_fresh AS c3
            ON c3.contact_id = s2.origin
        LEFT JOIN story_fresh AS s4
            ON s4.type = 'inservice'
            AND s4.destination = s2.origin
        LEFT JOIN contacts_fresh AS c5
            ON c5.contact_id = s4.origin
        WHERE s.type = 'sales'
          AND s.created_at >= '2025-06-01'
          AND s.status != 'duplicate'
          AND s.status != 'spam'
          AND c5.email LIKE '%motusnova%'
          AND c5.contact_id=408182
    )
    SELECT DISTINCT ON (story_id)
        story_id,
        patient_contact_id,
        patient_first_name,
        patient_last_name,
        salesperson_contact_id,
        salesperson_email,
        salesperson_first_name,
        status,
        sales_story_created_at,
        age_in_days,
        clinic_name
    FROM sales_story_age
    WHERE status IN (
        'strongLink', 'weakLink', 'WN', 'unclaim', 'spam', 'scheduled', 'identified'
    )
    AND age_in_days > 14
    ORDER BY story_id, sales_story_created_at ASC;
    '''

    dbCursor.execute(query)
    columnNames = [desc[0] for desc in dbCursor.description]
    rows = dbCursor.fetchall()
    df = pd.DataFrame(rows, columns=columnNames)
    return df

def getPatientsOver90DaysForSalespersonReport(dbCursor):
    query = '''
    WITH age_calculations AS (
        SELECT 
            s.destination,
            s.age_id,
            sf_ins.status,
            sf_ins.destination AS patient_contact_id,
            stuck.earliest_stuck_update as stuck_created_at,
            ins.latest_insurance_update,
            ins.earliest_insurance_update,
            a.begin_time,
            i.prescription,
            i.medical_records,
            i.primary_insurance_card,
            i.delivery_ticket,
            c.first_name AS patient_first_name,
            c.last_name AS patient_last_name,
            c5.contact_id AS salesperson_contact_id,
            c5.email AS salesperson_email,
            c5.first_name AS salesperson_first_name,
            c2.first_name AS doctor_first_name,
            c2.last_name AS doctor_last_name,
            c2.phone_number AS doctor_phone,
            c2.doc_fax AS doctor_fax,
            c2.email AS doctor_email,
            s_confirm.story_id AS confirm_fax_id,
            s_confirm.status AS confirm_fax_status,
            c_mrs.contact_id AS mrs_contact_id,
            c_mrs.first_name AS mrs_first_name,
            c_mrs.last_name AS mrs_last_name,
            c_clinic.first_name AS clinic_name,
            CASE 
                WHEN s.age_id IS NULL THEN 
                    EXTRACT(EPOCH FROM (NOW() - stuck.earliest_stuck_update)) / 86400
                ELSE 
                    EXTRACT(EPOCH FROM (NOW() - a.begin_time)) / 86400
            END AS stuck_age_in_days,
            CASE 
                WHEN sf_ins.age_id IS NULL THEN 
                    EXTRACT(EPOCH FROM (NOW() - ins.latest_insurance_update)) / 86400
                ELSE 
                    EXTRACT(EPOCH FROM (NOW() - a2.begin_time)) / 86400
            END AS insurance_age_in_days
        FROM story_fresh AS s
        LEFT JOIN insurance_fresh AS i
            ON i.insurance_id = s.destination
        left join story_fresh sf_ins
        on s.destination = sf_ins.story_id and sf_ins.type = 'insurance'
        LEFT JOIN contacts_fresh AS c
            ON c.contact_id = sf_ins.destination
        LEFT JOIN age_table AS a
            ON a.age_id = s.age_id
        LEFT JOIN (
            SELECT 
                story_id,
                MAX(created_at) AS latest_insurance_update,
                MIN(created_at) as earliest_insurance_update
            FROM story
            WHERE type = 'insurance'
                AND status IS NOT NULL
                AND status <> 'duplicate'
            GROUP BY story_id
        ) ins
            ON ins.story_id = s.destination
        LEFT JOIN (
            SELECT 
                story_id,
                MAX(created_at) AS latest_stuck_update,
                Min(created_at) as earliest_stuck_update
            FROM story
            WHERE type = 'stuck'
                AND status IS NOT NULL
                AND status not IN ('duplicate', 'complete')
            GROUP BY story_id
        ) stuck
            ON stuck.story_id = s.story_id
        LEFT JOIN story_fresh AS s3
            ON s3.type = 'clinicPatient'
            AND s3.destination = sf_ins.destination
        LEFT JOIN contacts_fresh AS c_clinic
            ON c_clinic.contact_id = s3.origin
        LEFT JOIN LATERAL (
            SELECT s4.origin, s4.destination
            FROM story_fresh AS s4
            WHERE s4.type = 'inservice'
                AND s4.destination = s3.origin
            ORDER BY s4.created_at DESC
            LIMIT 1
        ) s4_latest ON true
        LEFT JOIN contacts_fresh AS c5
            ON c5.contact_id = s4_latest.origin
        LEFT JOIN story_fresh AS s2 
            ON s2.type = 'prescriberFax' 
            AND sf_ins.destination = s2.destination
        LEFT JOIN contacts_fresh AS c2 
            ON s2.origin = c2.contact_id
        LEFT JOIN story_fresh AS s_confirm
            ON s_confirm.type = 'confirmFax'
            AND s_confirm.origin = s.destination
            AND s_confirm.status = 'Confirmed'
        LEFT JOIN story_fresh AS s_claims
            ON s_claims.type = 'claimsDocs'
            AND s_claims.destination = s.destination
        LEFT JOIN contacts_fresh AS c_mrs
            ON c_mrs.contact_id = s_claims.origin
        left join age_table a2
            on a2.age_id = sf_ins.age_id
        WHERE s.type = 'stuck'
            AND ins.earliest_insurance_update >= '2025-06-01'
            AND c5.email LIKE '%motusnova%'
            AND c5.contact_id=408182
    )
    SELECT DISTINCT ON (destination)
        destination,
        age_id,
        status,
        patient_contact_id,
        patient_first_name,
        patient_last_name,
        earliest_insurance_update,
        stuck_age_in_days,
        insurance_age_in_days,
        prescription,
        medical_records,
        primary_insurance_card,
        delivery_ticket,
        salesperson_contact_id,
        salesperson_email,
        salesperson_first_name,
        doctor_first_name,
        doctor_last_name,
        doctor_phone,
        doctor_fax,
        doctor_email,
        confirm_fax_id,
        confirm_fax_status,
        mrs_contact_id,
        mrs_first_name,
        mrs_last_name,
        clinic_name
    FROM age_calculations
    WHERE insurance_age_in_days > 90
    AND status NOT IN ('EOB', 'duplicate', 'submitted', 'sixtyDaysFree', 'rejectAuth', 'notCovered', 'deliveryTicket', 'cold', 'returned','optout','sixtyDayFree','cash')
    ORDER BY destination, earliest_insurance_update ASC;
    '''
    
    dbCursor.execute(query)
    columnNames = [desc[0] for desc in dbCursor.description]
    rows = dbCursor.fetchall()
    df = pd.DataFrame(rows, columns=columnNames)
    return df

def getFailedInformedConsentReport(dbCursor):
    query = '''
    WITH consent_data AS (
        SELECT
            sf_pss.destination AS profile_id,
            sf_pss.status,
            dt_stuck.created_at AS status_updated_at,
            c.first_name AS patient_first_name,
            c.last_name AS patient_last_name,
            c.contact_id AS patient_contact_id,
            c5.contact_id AS salesperson_contact_id,
            c5.email AS salesperson_email,
            c5.first_name AS salesperson_first_name,
            c_clinic.first_name AS clinic_name,
            i.incomplete_delivery_ticket_url,
            EXTRACT(EPOCH FROM (NOW() - dt_stuck.created_at)) / 86400 AS days_in_status
        FROM story_fresh AS sf_pss
        LEFT JOIN story_fresh AS sf_np
            ON sf_np.destination = sf_pss.destination
            AND sf_np.type = 'newProfile'
        LEFT JOIN story_fresh AS sf_ins
            ON sf_ins.story_id = sf_np.origin
            AND sf_ins.type = 'insurance'
        LEFT JOIN contacts_fresh AS c
            ON c.contact_id = sf_ins.destination
        LEFT JOIN story_fresh AS s3
            ON s3.type = 'clinicPatient'
            AND s3.destination = sf_ins.destination
        LEFT JOIN contacts_fresh AS c_clinic
            ON c_clinic.contact_id = s3.origin
        LEFT JOIN insurance_fresh AS i
            ON i.insurance_id = sf_np.origin
        LEFT JOIN (
            SELECT story_id, MIN(created_at) AS created_at
            FROM story
            WHERE type = 'insurance'
                AND status = 'deliveryTicket'
            GROUP BY story_id
        ) dt_stuck
            ON dt_stuck.story_id = sf_ins.story_id
        LEFT JOIN LATERAL (
            SELECT s4.origin
            FROM story_fresh AS s4
            WHERE s4.type = 'inservice'
                AND s4.destination = s3.origin
            ORDER BY s4.created_at DESC
            LIMIT 1
        ) s4_latest ON true
        LEFT JOIN contacts_fresh AS c5
            ON c5.contact_id = s4_latest.origin
        WHERE sf_pss.type = 'patientSuccessStory'
            AND sf_pss.status IN ('orderCanceled', 'salesHandover', 'salesEscalation')
            AND c5.email LIKE '%motusnova%'
            AND dt_stuck.created_at >= '2026-01-28'
            AND c5.contact_id=408182
    )
    SELECT DISTINCT ON (profile_id)
        profile_id,
        status,
        status_updated_at,
        patient_first_name,
        patient_last_name,
        patient_contact_id,
        salesperson_contact_id,
        salesperson_email,
        salesperson_first_name,
        clinic_name,
        days_in_status,
        incomplete_delivery_ticket_url
    FROM consent_data
    ORDER BY profile_id, status_updated_at ASC;
    '''

    dbCursor.execute(query)
    columnNames = [desc[0] for desc in dbCursor.description]
    rows = dbCursor.fetchall()
    df = pd.DataFrame(rows, columns=columnNames)
    return df

def hasDoctorContactInfo(row):
    """Check if we have complete doctor contact info and confirmed fax"""
    has_doctor_name = (not pd.isna(row['doctor_first_name']) or not pd.isna(row['doctor_last_name']))
    has_doctor_phone = not pd.isna(row['doctor_phone'])
    has_doctor_email = not pd.isna(row['doctor_email'])
    has_confirmed_fax = not pd.isna(row['confirm_fax_id']) and row['confirm_fax_status'] == 'Confirmed'
    
    return has_doctor_name and has_doctor_phone and has_doctor_email and has_confirmed_fax

def formatDoctorInfo(row):
    """Format doctor contact information for display"""
    parts = []
    
    # Doctor name
    doctor_name = f"{row['doctor_first_name'] or ''} {row['doctor_last_name'] or ''}".strip()
    if doctor_name:
        parts.append(doctor_name)
    
    # Phone number
    if not pd.isna(row['doctor_phone']) and row['doctor_phone']:
        parts.append(f"Ph: {row['doctor_phone']}")
    
    # Fax number - always show, even if N/A
    if not pd.isna(row['doctor_fax']) and row['doctor_fax']:
        parts.append(f"Fax: {row['doctor_fax']}")
    else:
        parts.append(f"Fax: N/A")
    
    if parts:
        return "<br>".join(parts)
    else:
        return "N/A"

def determineAction(row):
    """Determine the action needed based on status and conditions"""
    status = row['status']
    
    # For prescription/medical records statuses
    if status in ['needPrescriptionAndMedicalRecords', 'needPrescriptionOnly', 'needMedicalRecordsOnly']:
        # Check if medical records specialist is assigned
        has_mrs = not pd.isna(row['mrs_contact_id'])
        
        if not has_mrs:
            # No MRS assigned - check if we need doctor info
            if hasDoctorContactInfo(row):
                return 'contactDoc', None
            else:
                return 'confirmDocReceivedFax', None
        else:
            # MRS assigned
            mrs_name = f"{row['mrs_first_name'] or ''} {row['mrs_last_name'] or ''}".strip()
            return 'followUpWithMedicalRecordSpecialist', mrs_name
    
    # For insuranceCard status
    elif status == 'insuranceCard':
        has_insurance_card = not pd.isna(row['primary_insurance_card'])
        if not has_insurance_card:
            return 'needInsuranceCard', None
        else:
            return 'cardNotActive', None
    
    # For insuranceTerminated status
    elif status == 'insuranceTerminated':
        return 'cardNotActive', None
    
    # For needDoctor status
    elif status == 'needDoctor':
        return 'getDocInfo', None
    
    # For special statuses requiring sixty day check
    elif status in ['reject', 'rejectAuth', 'close', 'needDME', 'notCovered']:
        return 'checkSixtyDays', None
    
    # For newly added statuses
    elif status in ['missingPatientInfo', 'missingProductInfo', 'insuranceVerification', 
                    'requestedInfoAdded', 'auth', 'telehealth', 'HMO', 'info', 'readyToBill', 
                    'deliveryTicket', 'submitted', 'newClaim']:
        return 'contactTristin', None
    
    return 'unknown', None

def getActionLegend():
    """Return HTML legend explaining action statuses"""
    return '''
    <div style="margin-top: 30px; padding: 20px; background-color: #f5f5f5; border-radius: 5px;">
        <h3 style="margin-top: 0;">Action Legend:</h3>
        <ul style="line-height: 1.8;">
            <li><strong>getDocInfo:</strong> Reach out to the patient/family/referring provider to see if they can help establish a physician, NP, or PA that would be able to provide the prescription/medical notes needed. Right now no fax requests are being sent out for this patient. Once we have the appropriate contact info (phone and fax), then ask Parth to update their status accordingly.</li>
            <li><strong>confirmDocReceivedFax:</strong> We need to make sure we have the correct fax number and need to confirm the doctor's office has received our fax request.</li>
            <li><strong>contactDoc:</strong> We've confirmed we have the right fax number previously. Make sure they received our fax in the past or send fax to a new fax number you get and make sure they receive it, and add urgency in getting the paperwork completed. <strong>Alternate options</strong> to explore if other things are not working: see if there's a way we can email them instead, see if there's a fax number that goes directly to the medical assistant vs some large center where things might get lost, see if they use parachute (if they do then see if they can find Bestcare, Kesslick, or Independent Medical in the system and let Parth know), drop by the doctor's office in person with a physical copy of what we need completed, contact patient to see if they can somehow help us get the information and talk to their doctor -- this will help expedite the process potentially (good thing to do in all situations if possible).</li>
            <li><strong>followUpWithMedicalRecordSpecialist:</strong> Follow up with the medical records specialist (name provided in parentheses) to see what you can do to help and to understand the current roadblock if any.</li>
            <li><strong>needInsuranceCard:</strong> We need front and back pictures of the patient's primary insurance card. The patient has been emailed and texted about this, maybe even called but we have not been able to successfully get what is needed. Please call the patient, their clinic, and/or doctor to get their insurance card picture(s). You can use the provided link which they can use to upload the pictures if that helps.</li>
            <li><strong>cardNotActive:</strong> We have primary insurance info but it is not showing as active info, so we need brand new info that is different from the existing info OR We have primary insurance info, but this is not their primary. It's their secondary. We need their primary info. It's possible the patient is confused. They might think their medicare card is their primary, but if they have medicare advantage then we need the private insurance company card, which is their real primary insurance info.</li>
            <li><strong>contactTristin:</strong> Contact Tristin to discuss the current status, identify any roadblocks, and determine what actions can be taken to advance this case. Create a Google Chat group with Tristin and Divyesh to coordinate next steps.</li>
        </ul>
    </div>
    '''

def getSixtyDaysActionLegend():
    """Return HTML legend explaining sixty days action status"""
    return '''
    <div style="margin-top: 20px; padding: 20px; background-color: #e8f5e9; border-radius: 5px; border-left: 4px solid #4CAF50;">
        <h3 style="margin-top: 0;">Sixty Day Check Action Legend:</h3>
        <p><strong>checkSixtyDays:</strong> No immediate action is required here. If you have reason to believe this patient would genuinely benefit from the 60 days free program and it's worth pursuing, feel free to reach out, have a conversation with them, and guide them through completing this <a href="https://docs.google.com/forms/d/e/1FAIpQLSc3YXdXJ69Dh6bfCCyqTX97wkmR9wep9YxPOe0RN26nCdSy6w/viewform" target="_blank">form</a>. Otherwise, no follow-up is needed.</p>
    </div>
    '''

def getIncompleteSignUpActionLegend():
    """Return HTML legend explaining incomplete sign-up action statuses"""
    return '''
    <div style="margin-top: 20px; padding: 20px; background-color: #fff3cd; border-radius: 5px; border-left: 4px solid #f39c12;">
        <h3 style="margin-top: 0;">Incomplete Sign-Up Action Legend:</h3>
        <p><strong>completePatientSignUp:</strong> Either a patient is in progress of signing up but there's something that's slowing down the process, so this is being flagged to make sure you are aware and to make sure this has not fallen through the cracks. OR the patient's status needs to be updated to optout (not interested), lost (not able to get in touch and/or was identified but could never get a patient demo scheduled), or unqualified (patient is not appropriate) to accurately reflect the situation.</p>
        
        <h4>Context for each sales (patient sign up) status:</h4>
        <ul style="line-height: 1.8;">
            <li><strong>strongLink:</strong> They are scheduled to speak via calendly appointment with inside sales or are scheduled for a patient demo</li>
            <li><strong>weakLink:</strong> The patient has low interest and haven't been able to complete sign up and haven't been able to come to a conclusion that the patient is not interested in proceeding</li>
            <li><strong>WN:</strong> Wrong number. See if you can get correct contact information for the patient and complete sign-up/on-boarding</li>
            <li><strong>unclaim:</strong> Patient was referred but we have not been able to get in touch with the patient to get them started and complete sign up as a clinic referral. We have called multiple times, left voicemails, and texted multiple times.</li>
            <li><strong>spam:</strong> This is weird. The patient was identified or referred but didn't know who we are or was marked incorrectly. Follow up with inside sales (kailey) to get additional information.</li>
            <li><strong>scheduled:</strong> They are scheduled to speak via calendly appointment with inside sales or are scheduled for a patient demo</li>
            <li><strong>identified:</strong> You identified this patient, but no more progress has been made in getting a patient demo scheduled or getting the patient signed up. If you have lots of patients in this status, then please take a moment to go back and update their status to be accurate.</li>
        </ul>
    </div>
    '''

def getFailedInformedConsentActionLegend():
    """Return HTML legend explaining failed informed consent action statuses"""
    return '''
    <div style="margin-top: 20px; padding: 20px; background-color: #fdecea; border-radius: 5px; border-left: 4px solid #c0392b;">
        <h3 style="margin-top: 0;">Failed Informed Consent Action Legend:</h3>
        <ul style="line-height: 1.8;">
            <li><strong>patientOptedOut:</strong> Contact the patient to find out why they are not interested in continuing to get the Motus Hand/Foot.</li>
            <li><strong>contactPatient:</strong> Find other ways to contact patient apart from the information on file. Attempt to contact patient using new information found. New information can be found using the following: Go to the therapist/ clinic to confirm the patient's contact information, ask the therapist/ clinic for alternate contact information, check sign up notes that sales person might have made while signing patient up. If the sale person is able to get in touch with the patient, they should try and send the patient the incomplete delivery ticket link (provided in the above table), and then change the patientSuccess status to consentFailed using the 'Update Patient Success' form in rc_insuranceCheck. This will indicate to the inside sales team that they should try and continue getting in touch with this patient incase the patient has not signed the packet yet.</li>
            <li><strong>recommendedOutreach:</strong> Attempt to get in touch with the patient using the information on file. The inside sales team is also attempting contact using the information on file, however has been unsuccessful. If you can provide new information that is not on file, that would be helpful.</li>
        </ul>
    </div>
    '''

def getOver90DaysActionLegend():
    """Return HTML legend explaining 90+ days action status"""
    return '''
    <div style="margin-top: 20px; padding: 20px; background-color: #ffebee; border-radius: 5px; border-left: 4px solid #f44336;">
        <h3 style="margin-top: 0;">90+ Days Action Legend:</h3>
        <p><strong>moveToCold:</strong> These patients have been stuck in the system for over 90 days. Please attempt to contact the patient again. If you’ve already made multiple attempts without any progress, move the patient to Cold status.</p>
    </div>
    '''

def createHTMLTable(df_salesperson):
    """Create HTML table for salesperson's patients"""
    # Create a copy to avoid SettingWithCopyWarning
    df_salesperson = df_salesperson.copy()
    
    # Add action column for sorting
    df_salesperson['action_status'] = df_salesperson.apply(lambda row: determineAction(row)[0], axis=1)
    
    # Sort: non-followUpWithMedicalRecordSpecialist first, then followUpWithMedicalRecordSpecialist by sign up date
    df_salesperson['sort_key'] = df_salesperson['action_status'].apply(
        lambda x: 0 if x != 'followUpWithMedicalRecordSpecialist' else 1
    )
    df_salesperson = df_salesperson.sort_values(by=['sort_key', 'earliest_insurance_update'])
    
    html = '''
    <table style="border-collapse: collapse; width: 100%; font-size: 12px;">
        <thead>
            <tr style="background-color: #4CAF50; color: white;">
                <th style="border: 1px solid #ddd; padding: 8px;">ID</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Name</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Status</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Sign Up Date</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Days in Status</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Have Insurance Card?</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Doctor Contact Info</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Clinic Name</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Action</th>
                <th style="border: 1px solid #ddd; padding: 8px;">CRM Link</th>
            </tr>
        </thead>
        <tbody>
    '''
    
    for index, row in df_salesperson.iterrows():
        # Determine values
        patient_id = row['patient_contact_id']
        patient_name = f"{row['patient_first_name'] or ''} {row['patient_last_name'] or ''}".strip()
        status = row['status']
        created_date = row['earliest_insurance_update'].strftime('%Y-%m-%d') if not pd.isna(row['earliest_insurance_update']) else 'N/A'
        if pd.isna(row['stuck_age_in_days']):
            days_in_status = int(row['insurance_status_age_in_days']) if not pd.isna(row['insurance_status_age_in_days']) else 0
        else:
            days_in_status = int(row['stuck_age_in_days'])
        
        # Make "Yes" a hyperlink to the insurance card if it exists
        if not pd.isna(row['primary_insurance_card']):
            has_card = f'<a href="{row["primary_insurance_card"]}" target="_blank">Yes</a>'
        else:
            has_card = 'No'
        
        doctor_info = formatDoctorInfo(row)
        
        clinic_name = row['clinic_name'] if not pd.isna(row['clinic_name']) else 'N/A'
        
        action_status, mrs_name = determineAction(row)
        action_display = action_status
        if mrs_name:
            action_display = f"{action_status} ({mrs_name})"
        
        # Add link for needInsuranceCard action
        if action_status == 'needInsuranceCard':
            insurance_card_url = f"https://two.motusnova.com/MoInsCard/{row['destination']}"
            action_display = f'<a href="{insurance_card_url}" target="_blank">{action_status}</a>'
        
        crm_link = f"https://two.motusnova.com/viewBuilderApp/view-only/61da67fb-b01f-4f70-a0e8-72e808d9bf38=patient_contact_id:{patient_id}"
        
        # Create row with alternating colors
        row_color = '#f2f2f2' if index % 2 == 0 else 'white'
        html += f'''
            <tr style="background-color: {row_color};">
                <td style="border: 1px solid #ddd; padding: 8px;">{patient_id}</td>
                <td style="border: 1px solid #ddd; padding: 8px;">{patient_name}</td>
                <td style="border: 1px solid #ddd; padding: 8px;">{status}</td>
                <td style="border: 1px solid #ddd; padding: 8px;">{created_date}</td>
                <td style="border: 1px solid #ddd; padding: 8px;">{days_in_status}</td>
                <td style="border: 1px solid #ddd; padding: 8px;">{has_card}</td>
                <td style="border: 1px solid #ddd; padding: 8px;">{doctor_info}</td>
                <td style="border: 1px solid #ddd; padding: 8px;">{clinic_name}</td>
                <td style="border: 1px solid #ddd; padding: 8px;">{action_display}</td>
                <td style="border: 1px solid #ddd; padding: 8px;"><a href="{crm_link}" target="_blank">View</a></td>
            </tr>
        '''
    
    html += '''
        </tbody>
    </table>
    '''
    
    return html


def createIncompleteSignUpTable(df_salesperson):
    """Create HTML table for incomplete patient sign-ups"""
    # Create a copy to avoid SettingWithCopyWarning
    df_salesperson = df_salesperson.copy()
    df_salesperson = df_salesperson.sort_values(by=['sales_story_created_at'])

    html = '''
    <h3 style="margin-top:40px;">⚠️ Incomplete Patient Sign-Ups (Action: completePatientSignUp)</h3>
    <p>These patients have been identified but their sign-up process is delayed or requires attention. Please ensure these have not fallen through the cracks and update the status if appropriate.</p>
    <table style="border-collapse: collapse; width: 100%; font-size: 12px;">
        <thead>
            <tr style="background-color: #f39c12; color: white;">
                <th style="border: 1px solid #ddd; padding: 8px;">ID</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Patient Name</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Status</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Initial Interest Date</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Days Since Identified</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Clinic Name</th>
                <th style="border: 1px solid #ddd; padding: 8px;">CRM Link</th>
            </tr>
        </thead>
        <tbody>
    '''

    for index, row in df_salesperson.iterrows():
        patient_id = row['patient_contact_id']
        patient_name = f"{row['patient_first_name'] or ''} {row['patient_last_name'] or ''}".strip()
        created_date = row['sales_story_created_at'].strftime('%Y-%m-%d') if not pd.isna(row['sales_story_created_at']) else 'N/A'
        days_in_status = int(row['age_in_days'])
        clinic_name = row['clinic_name'] if not pd.isna(row['clinic_name']) else 'N/A'

        crm_link = f"https://two.motusnova.com/viewBuilderApp/view-only/61da67fb-b01f-4f70-a0e8-72e808d9bf38=patient_contact_id:{patient_id}"

        row_color = '#f2f2f2' if index % 2 == 0 else 'white'
        html += f'''
            <tr style="background-color: {row_color};">
                <td style="border: 1px solid #ddd; padding: 8px;">{patient_id}</td>
                <td style="border: 1px solid #ddd; padding: 8px;">{patient_name}</td>
                <td style="border: 1px solid #ddd; padding: 8px;">{row['status']}</td>
                <td style="border: 1px solid #ddd; padding: 8px;">{created_date}</td>
                <td style="border: 1px solid #ddd; padding: 8px;">{days_in_status}</td>
                <td style="border: 1px solid #ddd; padding: 8px;">{clinic_name}</td>
                <td style="border: 1px solid #ddd; padding: 8px;"><a href="{crm_link}" target="_blank">View</a></td>
            </tr>
        '''

    html += '''
        </tbody>
    </table>
    '''
    return html

def createFailedInformedConsentTable(df_salesperson):
    """Create HTML table for failed informed consent patients"""
    df_salesperson = df_salesperson.copy()
    df_salesperson = df_salesperson.sort_values(by=['status_updated_at'])

    html = '''
    <table style="border-collapse: collapse; width: 100%; font-size: 12px;">
        <thead>
            <tr style="background-color: #c0392b; color: white;">
                <th style="border: 1px solid #ddd; padding: 8px;">ID</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Patient Name</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Status</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Ready to Ship Date</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Days in Status</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Clinic Name</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Incomplete Delivery Ticket</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Action</th>
                <th style="border: 1px solid #ddd; padding: 8px;">CRM Link</th>
            </tr>
        </thead>
        <tbody>
    '''

    for index, row in df_salesperson.iterrows():
        patient_id = row['patient_contact_id']
        patient_name = f"{row['patient_first_name'] or ''} {row['patient_last_name'] or ''}".strip()
        status = row['status']
        ready_to_ship_date = row['status_updated_at'].strftime('%Y-%m-%d') if not pd.isna(row['status_updated_at']) else 'N/A'
        days_in_status = int(row['days_in_status']) if not pd.isna(row['days_in_status']) else 0
        clinic_name = row['clinic_name'] if not pd.isna(row['clinic_name']) else 'N/A'
        if not pd.isna(row['incomplete_delivery_ticket_url']) and row['incomplete_delivery_ticket_url']:
            delivery_ticket = f'<a href="{row["incomplete_delivery_ticket_url"]}" target="_blank">View</a>'
        else:
            delivery_ticket = 'N/A'

        if status == 'orderCanceled':
            action = 'patientOptedOut'
        elif status == 'salesHandover':
            action = 'contactPatient'
        elif status == 'salesEscalation':
            action = 'recommendedOutreach'
        else:
            action = 'unknown'

        crm_link = f"https://two.motusnova.com/viewBuilderApp/view-only/61da67fb-b01f-4f70-a0e8-72e808d9bf38=patient_contact_id:{patient_id}"

        row_color = '#f2f2f2' if index % 2 == 0 else 'white'
        html += f'''
            <tr style="background-color: {row_color};">
                <td style="border: 1px solid #ddd; padding: 8px;">{patient_id}</td>
                <td style="border: 1px solid #ddd; padding: 8px;">{patient_name}</td>
                <td style="border: 1px solid #ddd; padding: 8px;">{status}</td>
                <td style="border: 1px solid #ddd; padding: 8px;">{ready_to_ship_date}</td>
                <td style="border: 1px solid #ddd; padding: 8px;">{days_in_status}</td>
                <td style="border: 1px solid #ddd; padding: 8px;">{clinic_name}</td>
                <td style="border: 1px solid #ddd; padding: 8px;">{delivery_ticket}</td>
                <td style="border: 1px solid #ddd; padding: 8px;">{action}</td>
                <td style="border: 1px solid #ddd; padding: 8px;"><a href="{crm_link}" target="_blank">View</a></td>
            </tr>
        '''

    html += '''
        </tbody>
    </table>
    '''
    return html

def createOver90DaysTable(df_salesperson):
    """Create HTML table for 90+ days patients"""
    # Create a copy to avoid SettingWithCopyWarning
    df_salesperson = df_salesperson.copy()
    df_salesperson = df_salesperson.sort_values(by=['earliest_insurance_update'])

    html = '''
    <h3 style="margin-top:40px;">❄️ Patients Over 90 Days (Action: moveToCold)</h3>
    <p>These patients have been stuck in the system for over 90 days. Please move them to 'cold' insurance status.</p>
    <table style="border-collapse: collapse; width: 100%; font-size: 12px;">
        <thead>
            <tr style="background-color: #f44336; color: white;">
                <th style="border: 1px solid #ddd; padding: 8px;">ID</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Name</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Status</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Sign Up Date</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Days in Current Status</th>
                <th style="border: 1px solid #ddd; padding: 8px;">Clinic Name</th>
                <th style="border: 1px solid #ddd; padding: 8px;">CRM Link</th>
            </tr>
        </thead>
        <tbody>
    '''

    for index, row in df_salesperson.iterrows():
        patient_id = row['patient_contact_id']
        patient_name = f"{row['patient_first_name'] or ''} {row['patient_last_name'] or ''}".strip()
        status = row['status']
        created_date = row['earliest_insurance_update'].strftime('%Y-%m-%d') if not pd.isna(row['earliest_insurance_update']) else 'N/A'
        days_since_signup = int(row['insurance_age_in_days'])
        clinic_name = row['clinic_name'] if not pd.isna(row['clinic_name']) else 'N/A'

        crm_link = f"https://two.motusnova.com/viewBuilderApp/view-only/61da67fb-b01f-4f70-a0e8-72e808d9bf38=patient_contact_id:{patient_id}"

        row_color = '#f2f2f2' if index % 2 == 0 else 'white'
        html += f'''
            <tr style="background-color: {row_color};">
                <td style="border: 1px solid #ddd; padding: 8px;">{patient_id}</td>
                <td style="border: 1px solid #ddd; padding: 8px;">{patient_name}</td>
                <td style="border: 1px solid #ddd; padding: 8px;">{status}</td>
                <td style="border: 1px solid #ddd; padding: 8px;">{created_date}</td>
                <td style="border: 1px solid #ddd; padding: 8px;">{days_since_signup}</td>
                <td style="border: 1px solid #ddd; padding: 8px;">{clinic_name}</td>
                <td style="border: 1px solid #ddd; padding: 8px;"><a href="{crm_link}" target="_blank">View</a></td>
            </tr>
        '''

    html += '''
        </tbody>
    </table>
    '''
    return html

def sendSalespersonEmail(salesperson_email, salesperson_name, salesperson_contact_id, html_table, incomplete_html_table, patient_count, incomplete_count, html_table_sixty_days=None, sixty_days_count=0, html_table_over_90_days=None, over_90_days_count=0, failed_consent_html_table='', failed_consent_count=0):
    """Send email to salesperson with their patients"""
    try:
        email_user, email_password = get_mail_credentials()
        
        msg = MIMEMultipart()
        msg['From'] = 'service@motusnova.com'
        msg['To'] = 'manav.jain@motusnova.com'#salesperson_email
        #msg['Cc'] = 'parth.patel@motusnova.com'
        msg['Subject'] = f'Patient Follow-Up Report - {patient_count + incomplete_count} Patients Requiring Action'
        
        # Build sixty days section if exists
        sixty_days_section = ''
        if html_table_sixty_days:
            sixty_days_section = f'''
            <h3 style="margin-top: 40px;">Sixty Day Check Required ({sixty_days_count} patients) --- LOW PRIORITY</h3>
            <p>The following patients require sixty day check action:</p>
            {html_table_sixty_days}
            {getSixtyDaysActionLegend()}
            '''
        
        # Build over 90 days section if exists
        over_90_days_section = ''
        if html_table_over_90_days:
            over_90_days_section = f'''
            <h3 style="margin-top: 40px;">Patients Over 90 Days ({over_90_days_count} patients) --- LOW PRIORITY</h3>
            <p>The following patients have been stuck in the system for over 90 days:</p>
            {html_table_over_90_days}
            {getOver90DaysActionLegend()}
            '''

        failed_consent_section = ''
        if failed_consent_html_table:
            failed_consent_section = f'''
            <h3 style="margin-top: 40px;">Failed Informed Consent ({failed_consent_count} patients)</h3>
            <p>Informed Consent is the stage where an inside sales representative contacts patients to confirm that they understand the insurance and billing process and agree to receive the device. During this conversation, the representative also sets expectations and addresses any questions or concerns the patient may have. For the patients listed below, we were either unable to reach them or they chose to opt out of proceeding.</p>
            {failed_consent_html_table}
            {getFailedInformedConsentActionLegend()}
            '''
        
        # Create full email body
        email_body = f'''
        <html>
        <body style="font-family: Arial, sans-serif;">
            <h2>Patient Follow-Up Report</h2>
            <p>Hi {salesperson_name},</p>
            <p>You have <strong>{patient_count + incomplete_count}</strong> patient(s) requiring follow-up action. Please review the details below:</p>
            <p style="margin-top: 20px;"><strong>Important: If you have notes/updates after following up on these patients, please update the insurance status notes in the CRM (do NOT change the status itself), and update the respective information about the patient/doctor as needed. If you believe there should be a status change, please message Parth.</strong></p>
            {html_table}
            {getActionLegend()}
            {incomplete_html_table}
            {getIncompleteSignUpActionLegend()}
            {failed_consent_section}
            {sixty_days_section}
            {over_90_days_section}
            <p style="margin-top: 30px;">If you have any questions, please reach out to the team.</p>
            <p>Best regards,<br>Motus Nova Team</p>
        </body>
        </html>
        '''
        
        msg.attach(MIMEText(email_body, 'html'))
        
        # Send email
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(email_user, email_password)
        server.send_message(msg)
        server.quit()
        
        print(f"✓ Email sent successfully to {salesperson_email} ({patient_count + incomplete_count} patients)")
        return True
        
    except Exception as e:
        print(f"✗ Failed to send email to {salesperson_email}: {e}")
        return False

def main():
    print("=== Starting TEST Salesperson Patient Report Script ===")
    print("=== FILTERING FOR SALESPERSON CONTACT_IDs: 129873, 129876, 303820, 303821 ===")
    print("===   Mirasol Jacobs (129873), Alyssa Carmell (129876),")
    print("===   Katie Howard (303820), Leslie Kvinge (303821) ===\n")
    
    dbConnection, dbCursor = createConnection()
    
    print("Fetching patient data from database...")
    patients_df = getPatientsForSalespersonReport(dbCursor)
    incomplete_signups_df = getPatientsWithIncompleteSignUpReport(dbCursor)
    over_90_days_df = getPatientsOver90DaysForSalespersonReport(dbCursor)
    failed_consent_df = getFailedInformedConsentReport(dbCursor)
    print(f"Found {len(patients_df)} patients requiring follow-up\n")
    
    if len(patients_df) == 0 and len(incomplete_signups_df) == 0 and len(failed_consent_df) == 0 and len(over_90_days_df) == 0:
        print("No patients to report for these salespeople. Exiting.")
        dbCursor.close()
        dbConnection.close()
        return
    
    # Print patient details for debugging
    print("=== Patient Details by Salesperson ===")
    for salesperson_id in [129873, 129876, 303820, 303821]:
        salesperson_patients = patients_df[patients_df['salesperson_contact_id'] == salesperson_id]
        if len(salesperson_patients) > 0:
            first_row = salesperson_patients.iloc[0]
            print(f"\nSalesperson: {first_row['salesperson_first_name']} (ID: {salesperson_id}, Email: {first_row['salesperson_email']})")
            print(f"Patients: {len(salesperson_patients)}")
            for index, row in salesperson_patients.iterrows():
                days = int(row['insurance_status_age_in_days']) if pd.isna(row['stuck_age_in_days']) else int(row['stuck_age_in_days'])
                print(f"  - {row['patient_first_name']} {row['patient_last_name']} (Status: {row['status']}, Days: {days})")
        else:
            print(f"\nSalesperson ID {salesperson_id}: No patients found")
    
    # Group by salesperson
    grouped = patients_df.groupby('salesperson_email')
    
    emails_sent = 0
    emails_failed = 0
    
    print(f"\n=== Preparing to send emails ===")
    
    for salesperson_email, group_df in grouped:
        if pd.isna(salesperson_email) or salesperson_email == '':
            print(f"⊗ Skipping {len(group_df)} patients with no salesperson email")
            continue
        
        salesperson_name = group_df.iloc[0]['salesperson_first_name'] or 'there'
        salesperson_contact_id = group_df.iloc[0]['salesperson_contact_id']
        
        # Split patients into two groups
        sixty_days_statuses = ['reject', 'rejectAuth', 'close', 'needDME', 'notCovered']
        regular_patients_df = group_df[~group_df['status'].isin(sixty_days_statuses)]
        sixty_days_patients_df = group_df[group_df['status'].isin(sixty_days_statuses)]
        
        patient_count = len(regular_patients_df)
        sixty_days_count = len(sixty_days_patients_df)
        
        print(f"\n--- Processing salesperson: {salesperson_name} ({salesperson_email}, ID: {salesperson_contact_id}) ---")
        print(f"    {patient_count} regular patient(s), {sixty_days_count} sixty-day check patient(s)")
        
        # Create HTML table for regular patients
        html_table = createHTMLTable(regular_patients_df) if patient_count > 0 else ''
        
        # Create HTML table for sixty days patients
        html_table_sixty_days = createHTMLTable(sixty_days_patients_df) if sixty_days_count > 0 else None
        
        # Get incomplete signups
        salesperson_incomplete = incomplete_signups_df[incomplete_signups_df['salesperson_contact_id'] == salesperson_contact_id]
        incomplete_count = len(salesperson_incomplete)
        incomplete_html_table = createIncompleteSignUpTable(salesperson_incomplete) if len(salesperson_incomplete) > 0 else ''

        # Get failed informed consent patients
        salesperson_failed_consent = failed_consent_df[failed_consent_df['salesperson_contact_id'] == salesperson_contact_id]
        failed_consent_count = len(salesperson_failed_consent)
        failed_consent_html_table = createFailedInformedConsentTable(salesperson_failed_consent) if failed_consent_count > 0 else ''

        # Get over 90 days patients
        salesperson_over_90_days = over_90_days_df[over_90_days_df['salesperson_contact_id'] == salesperson_contact_id]
        over_90_days_count = len(salesperson_over_90_days)
        html_table_over_90_days = createOver90DaysTable(salesperson_over_90_days) if len(salesperson_over_90_days) > 0 else None

        # Send email
        if sendSalespersonEmail(salesperson_email, salesperson_name, salesperson_contact_id, html_table, incomplete_html_table, patient_count, incomplete_count, html_table_sixty_days, sixty_days_count, html_table_over_90_days, over_90_days_count, failed_consent_html_table, failed_consent_count):
            emails_sent += 1
        else:
            emails_failed += 1
    
    print(f"\n=== TEST Summary ===")
    print(f"Salesperson Contact IDs: 129873, 129876, 303820, 303821")
    print(f"Total patients: {len(patients_df)}")
    print(f"Emails sent: {emails_sent}")
    print(f"Emails failed: {emails_failed}")
    
    dbCursor.close()
    dbConnection.close()
    print("\n=== TEST Script Complete ===")

if __name__ == "__main__":
    main()