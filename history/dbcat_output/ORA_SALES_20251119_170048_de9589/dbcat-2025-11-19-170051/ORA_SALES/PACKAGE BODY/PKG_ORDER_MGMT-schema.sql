DELIMITER $$

CREATE OR REPLACE PACKAGE BODY           PKG_ORDER_MGMT AS
    PROCEDURE QUEUE_ORDER(p_order_id NUMBER, p_status VARCHAR2) IS
    BEGIN
        UPDATE ORA_SALES.ORDER_FACT
           SET STATUS = p_status,
               UPDATED_AT = SYSDATE
         WHERE ORDER_ID = p_order_id;
    END;

    PROCEDURE CLOSE_ORDER(p_order_id NUMBER) IS
    BEGIN
        UPDATE ORA_SALES.ORDER_FACT
           SET STATUS = 'C',
               UPDATED_AT = SYSDATE
         WHERE ORDER_ID = p_order_id;
    END;

    FUNCTION COUNT_BY_STATUS(p_status VARCHAR2) RETURN NUMBER IS
        v_cnt NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_cnt
          FROM ORA_SALES.ORDER_FACT
         WHERE STATUS = p_status;
        RETURN v_cnt;
    END;
END PKG_ORDER_MGMT;
$$

