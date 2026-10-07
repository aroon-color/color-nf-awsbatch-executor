package com.color.nextflow.tieredbatch

import nextflow.Session
import nextflow.trace.TraceObserver
import nextflow.trace.TraceObserverFactory

class BatchReportingFactory implements TraceObserverFactory {
    @Override
    Collection<TraceObserver> create(Session session) {
        if (session.config.navigate('tieredAwsBatch.reporting') != true) return []
        try { return [new BatchReportingObserver(session)] }
        catch (IOException error) {
            java.util.logging.Logger.getLogger(getClass().name).warning('Unable to initialize AWS Batch report files')
            return []
        }
    }
}
