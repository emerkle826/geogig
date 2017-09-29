/* Copyright (c) 2016 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 *
 * Contributors:
 * Johnathan Garrett - initial implementation
 */
package org.locationtech.geogig.geotools.plumbing;

import org.geotools.data.DataStore;
import org.locationtech.geogig.model.RevCommit;

public class DefaultDataStoreImportOp extends DataStoreImportOp<RevCommit> {

    @Override
    protected RevCommit callInternal() {
        final DataStore dataStore = dataStoreSupplier.get();

        RevCommit revCommit;

        /**
         * Import needs to: 1) Import the data 2) Add changes to be staged 3) Commit staged changes
         */
        try {
            // import data into the repository
            final ImportOp importOp = getImportOp();
            importOp.setProgressListener(getProgressListener());
            importOp.call();
            // add the imported data to the staging area
            callAdd();
            // commit the staged changes
            revCommit = callCommit();
        } finally {
            dataStore.dispose();
            dataStoreSupplier.cleanupResources();
        }

        return revCommit;
    }

}
