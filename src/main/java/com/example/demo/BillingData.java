package com.example.demo;

public record BillingData(
        int dataYear,
        int dataMonth,
        int accountId,
        String phoneNumber,
        Float dataUsage,
        int callDuration,

        int smsCount

) {
}
