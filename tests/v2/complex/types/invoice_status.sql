CREATE TYPE complex.invoice_status AS ENUM (
    'draft',
    'sent',
    'paid',
    'overdue',
    'cancelled'
); 