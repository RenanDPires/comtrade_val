INSERT INTO osci.def_ied (ied_id, sub_id)
VALUES
  ('SPSTIN_MI_CV1', 'SE_SPSTIN' ),
  ('SPSTIN_MI_CV2', 'SE_SPSTIN' ),
  ('SPSTIN_MI_CV3', 'SE_SPSTIN' ),
  ('SPSTIN_MI_CV4', 'SE_SPSTIN' ),
  ('SPSTIN_MI_CV5', 'SE_SPSTIN' ),
  ('SPSTIN_MI_CV6', 'SE_SPSTIN' ),
  ('SPSTIN_MI_CV7', 'SE_SPSTIN' ),
  ('SPSTIN_MI_CV8', 'SE_SPSTIN' );


INSERT INTO osci.def_expected_files (ied_id, ext)
VALUES
    ('SPSTIN_MI_CV1', '.cfg'),
    ('SPSTIN_MI_CV1', '.dat'),
    ('SPSTIN_MI_CV1', '.hdr'),

    ('SPSTIN_MI_CV2', '.cfg'),
    ('SPSTIN_MI_CV2', '.dat'),
    ('SPSTIN_MI_CV2', '.hdr'),

    ('SPSTIN_MI_CV3', '.cfg'),
    ('SPSTIN_MI_CV3', '.dat'),
    ('SPSTIN_MI_CV3', '.hdr'),

    ('SPSTIN_MI_CV4', '.cfg'),
    ('SPSTIN_MI_CV4', '.dat'),
    ('SPSTIN_MI_CV4', '.hdr'),

    ('SPSTIN_MI_CV5', '.cfg'),
    ('SPSTIN_MI_CV5', '.dat'),
    ('SPSTIN_MI_CV5', '.hdr'),

    ('SPSTIN_MI_CV6', '.cfg'),
    ('SPSTIN_MI_CV6', '.dat'),
    ('SPSTIN_MI_CV6', '.hdr'),

    ('SPSTIN_MI_CV7', '.cfg'),
    ('SPSTIN_MI_CV7', '.dat'),
    ('SPSTIN_MI_CV7', '.hdr'),

    ('SPSTIN_MI_CV8', '.cfg'),
    ('SPSTIN_MI_CV8', '.dat'),
    ('SPSTIN_MI_CV8', '.hdr');


INSERT INTO osci.def_digital_channels (ied_id, idx1, id_hint, description)
SELECT i.ied_id,
       d.idx1,
       d.id_hint,
       d.description
FROM osci.def_ied AS i
CROSS JOIN (
    VALUES
        (1, 'VO1', 'TRIGGER_OSC'),
        (2, 'CI2', 'FLH COMUT IND CF8'),
        (3, 'CI3', 'TRIP IND2 CF8')
) AS d(idx1, id_hint, description)
LEFT JOIN osci.def_digital_channels AS existing
       ON existing.ied_id = i.ied_id
      AND existing.idx1 = d.idx1
WHERE i.ied_id IN (
    'SPSTIN_MI_CV1',
    'SPSTIN_MI_CV2',
    'SPSTIN_MI_CV3',
    'SPSTIN_MI_CV4',
    'SPSTIN_MI_CV5',
    'SPSTIN_MI_CV6',
    'SPSTIN_MI_CV7',
    'SPSTIN_MI_CV8'
)
AND existing.ied_id IS NULL;  -- não duplica se já existir


INSERT INTO osci.def_ied (ied_id, sub_id)
VALUES
  ('PRSTF6_MI_CV1', 'SE_PRSTF6' ),
  ('PRSTF6_MI_CV2', 'SE_PRSTF6' ),
  ('PRSTF6_MI_CV3', 'SE_PRSTF6' ),
  ('PRSTF6_MI_CV4', 'SE_PRSTF6' ),
  ('PRSTF6_MI_CV5', 'SE_PRSTF6' ),
  ('PRSTF6_MI_CV6', 'SE_PRSTF6' ),
  ('PRSTF6_MI_CV7', 'SE_PRSTF6' ),
  ('PRSTF6_MI_CV8', 'SE_PRSTF6' );


INSERT INTO osci.def_expected_files (ied_id, ext)
VALUES
    ('PRSTF6_MI_CV1', '.cfg'),
    ('PRSTF6_MI_CV1', '.dat'),
    ('PRSTF6_MI_CV1', '.hdr'),

    ('PRSTF6_MI_CV2', '.cfg'),
    ('PRSTF6_MI_CV2', '.dat'),
    ('PRSTF6_MI_CV2', '.hdr'),

    ('PRSTF6_MI_CV3', '.cfg'),
    ('PRSTF6_MI_CV3', '.dat'),
    ('PRSTF6_MI_CV3', '.hdr'),

    ('PRSTF6_MI_CV4', '.cfg'),
    ('PRSTF6_MI_CV4', '.dat'),
    ('PRSTF6_MI_CV4', '.hdr'),

    ('PRSTF6_MI_CV5', '.cfg'),
    ('PRSTF6_MI_CV5', '.dat'),
    ('PRSTF6_MI_CV5', '.hdr'),

    ('PRSTF6_MI_CV6', '.cfg'),
    ('PRSTF6_MI_CV6', '.dat'),
    ('PRSTF6_MI_CV6', '.hdr'),

    ('PRSTF6_MI_CV7', '.cfg'),
    ('PRSTF6_MI_CV7', '.dat'),
    ('PRSTF6_MI_CV7', '.hdr'),

    ('PRSTF6_MI_CV8', '.cfg'),
    ('PRSTF6_MI_CV8', '.dat'),
    ('PRSTF6_MI_CV8', '.hdr');


INSERT INTO osci.def_digital_channels (ied_id, idx1, id_hint, description)
SELECT i.ied_id,
       d.idx1,
       d.id_hint,
       d.description
FROM osci.def_ied AS i
CROSS JOIN (
    VALUES
        (1, 'VO1', 'TRIGGER_OSC')
) AS d(idx1, id_hint, description)
LEFT JOIN osci.def_digital_channels AS existing
       ON existing.ied_id = i.ied_id
      AND existing.idx1 = d.idx1
WHERE i.ied_id IN (
    'PRSTF6_MI_CV1',
    'PRSTF6_MI_CV2',
    'PRSTF6_MI_CV3',
    'PRSTF6_MI_CV4',
    'PRSTF6_MI_CV5',
    'PRSTF6_MI_CV6',
    'PRSTF6_MI_CV7',
    'PRSTF6_MI_CV8'
)
AND existing.ied_id IS NULL;  -- não duplica se já existir

