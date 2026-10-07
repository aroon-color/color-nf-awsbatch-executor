package com.color.nextflow.tieredbatch.validation

import com.color.nextflow.tieredbatch.BatchReportingObserver
import nextflow.Session
import nextflow.trace.TraceObserver
import nextflow.trace.TraceObserverFactory

/** Deterministic host/rate metadata at the external boundary, never in the distribution. */
class ValidationReportingFactory implements TraceObserverFactory {
    @Override Collection<TraceObserver> create(Session session) {
        return [new BatchReportingObserver(session, { Map row ->
            [market: row.market == 'UNKNOWN' ? 'SPOT' : row.market, instanceId: row.market == 'ON_DEMAND' ? 'i-demand' : 'i-spot', instanceType: 'm5.large',
                availabilityZone: 'us-east-1a', hostVcpus: 2, hostMemoryMiB: 8192,
                hourlyRateUsd: row.market == 'ON_DEMAND' ? 0.096G : 0.03G]
        })]
    }
}
