-- ============================================================================
-- TELECOM SIM-CLONING FRAUD DETECTION QUERY
-- Binds the stream to a strict temporal window to catch geographical conflicts.
-- ============================================================================

SELECT 
    System.Timestamp AS WindowEnd,
    CS1.CallingIMSI,
    CS1.SwitchNum AS SwitchLocationA,
    CS2.SwitchNum AS SwitchLocationB,
    CS1.CallingNum AS VictimPhoneNumber,
    COUNT(*) AS FraudulentCallCount
INTO 
    [MyPBIOutput] -- Pushes directly to your Power BI custom streaming dataset
FROM 
    [CallStream] CS1 TIMESTAMP BY CallRecTime
JOIN 
    [CallStream] CS2 TIMESTAMP BY CallRecTime
    ON CS1.CallingIMSI = CS2.CallingIMSI
    -- Looks for calls matching the same identity that occur within 1 to 5 seconds of each other
    AND DATEDIFF(ss, CS1, CS2) BETWEEN 1 AND 5
WHERE 
    -- Flag it if the cellular routing switches are physically different
    CS1.SwitchNum != CS2.SwitchNum
GROUP BY 
    CS1.CallingIMSI, 
    CS1.SwitchNum, 
    CS2.SwitchNum, 
    CS1.CallingNum,
    TumblingWindow(Duration(second, 1))