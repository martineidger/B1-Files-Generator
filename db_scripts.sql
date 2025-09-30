CREATE TABLE Reports (
    id BIGSERIAL PRIMARY KEY,
    date_field DATE,
    eng_field VARCHAR(50),
    ru_field VARCHAR(50),
    number_field BIGINT,
    decimal_field DECIMAL(12, 8)
);

drop table Reports;

SELECT COUNT(*) FROM Reports;

delete from Reports;



CREATE OR REPLACE PROCEDURE calc_sum_and_median(OUT total_sum BIGINT, OUT median_value DECIMAL)
LANGUAGE plpgsql
AS $$
BEGIN
    -- сумма всех целых чисел
    SELECT SUM(number_field) INTO total_sum
    FROM Reports;

    -- медиана всех дробных чисел
    SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY decimal_field)
    INTO median_value
    FROM Reports
    WHERE decimal_field IS NOT NULL;
END;
$$;

CALL calc_sum_and_median(total_sum => NULL, median_value => NULL);

