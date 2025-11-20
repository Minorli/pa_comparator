DELIMITER $$

CREATE OR REPLACE PROCEDURE           SP_CREATE_ORDER (
    p_customer_code IN VARCHAR2,
    p_ship_method   IN VARCHAR2,
    p_amount        IN NUMBER,
    p_discount      IN NUMBER
) AS
    v_cust_id NUMBER;
    v_ship_id NUMBER;
BEGIN
    SELECT CUSTOMER_ID INTO v_cust_id FROM ORA_SALES.CUSTOMER_DIM
     WHERE CUSTOMER_CODE = p_customer_code;

    SELECT SHIP_METHOD_ID INTO v_ship_id FROM ORA_REF.SHIP_METHOD
     WHERE SHIP_METHOD_CODE = p_ship_method;

    INSERT INTO ORA_SALES.ORDER_FACT (
        ORDER_ID, ORDER_CODE, CUSTOMER_ID, SHIP_METHOD_ID,
        ORDER_TOTAL, DISCOUNT_RATE, STATUS, ORDER_NOTE
    )
    VALUES (
        ORA_SALES.SEQ_ORDER.NEXTVAL,
        'ORD-' || TO_CHAR(ORA_SALES.SEQ_ORDER.CURRVAL),
        v_cust_id,
        v_ship_id,
        p_amount,
        p_discount,
        'N',
        'Created via SP_CREATE_ORDER'
    );
END;
$$

