/* Copyright (c) 2017 Boundless and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * https://www.eclipse.org/org/documents/edl-v10.html
 * 
 * Contributors:
 * Erik Merkle (Boundless) - initial implementation
 */
package org.locationtech.geogig.rest.shp;

import static org.locationtech.geogig.cli.AbstractCommand.checkParameter;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.geotools.data.DataStore;
import org.geotools.data.shapefile.ShapefileDataStoreFactory;
import org.locationtech.geogig.cli.CommandFailedException;
import org.locationtech.geogig.geotools.plumbing.DataStoreImportOp;
import org.locationtech.geogig.geotools.plumbing.DataStoreImportOp.DataStoreSupplier;
import org.locationtech.geogig.geotools.plumbing.DefaultDataStoreImportOp;
import org.locationtech.geogig.repository.Context;
import org.locationtech.geogig.rest.geotools.DataStoreImportContextService;
import org.locationtech.geogig.web.api.ParameterSet;

import com.beust.jcommander.internal.Maps;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * Shapefile specific implementation of {@link DataStoreImportContextService}.
 */
public class ShpImportContext implements DataStoreImportContextService {

    private static final String SUPPORTED_FORMAT = "shp";

    private DataStoreImportOp.DataStoreSupplier dataStoreSupplier;

    @Override
    public boolean accepts(String format) {
        return SUPPORTED_FORMAT.equals(format);
    }

    @Override
    public DataStoreSupplier getDataStore(ParameterSet options) {
        if (dataStoreSupplier == null) {
            dataStoreSupplier = new ShpDataStoreSupplier(options);
        }
        return dataStoreSupplier;
    }

    @Override
    public String getCommandDescription() {
        return "Importing Shapfile.";
    }

    @Override
    public DataStoreImportOp<?> createCommand(Context context, ParameterSet options) {
        return context.command(DefaultDataStoreImportOp.class).setDataStore(getDataStore(options));
    }

    private static class ShpDataStoreSupplier implements DataStoreSupplier {

        private final File uploadedFile;
        private final String charset;
        private DataStore dataStore;

        private ShpDataStoreSupplier(ParameterSet options) {
            super();
            this.uploadedFile = getUploadFile(options);
            this.charset = options.getFirstValue("charset", StandardCharsets.ISO_8859_1.name());
        }

        private File getUploadFile(ParameterSet options) {
            // better be a shapefile
            File shapeFile = options.getUploadedFile();
            Preconditions.checkNotNull(shapeFile, "Shapefile is null");
            try {
                String canonicalPath = shapeFile.getCanonicalPath();
                // if the path ends with ".shp" we are good
                if (canonicalPath.endsWith(".shp")) {
                    return shapeFile;
                }
                // doesn't end in ".shp"
                // see if a file already exists with the extension appended to the end
                File newShapeFile = new File(canonicalPath + ".shp");
                if (!newShapeFile.exists()) {
                    // rename the uploadedFile to the new shapefile
                    boolean renameSucceeded = shapeFile.renameTo(newShapeFile);
                    if (renameSucceeded) {
                        return newShapeFile;
                    } else {
                        throw new RuntimeException("Failed to rename " + canonicalPath + " to "
                                + canonicalPath + ".shp");
                    }
                }
                throw new RuntimeException("Cannot rename "+ canonicalPath + " to "
                                + canonicalPath + ".shp, as the destination file already exists");
            } catch (Exception ex) {
                Throwables.propagate(ex);
            }
            return null;
        }

        @Override
        public void cleanupResources() {
            if (dataStore != null) {
                dataStore.dispose();
                dataStore = null;
            }
            if (uploadedFile != null) {
                uploadedFile.delete();
            }
        }

        @Override
        public DataStore get() {
            try {
                final ShapefileDataStoreFactory factory = new ShapefileDataStoreFactory();
                Map<String, Serializable> params = Maps.newHashMap();
                params.put(ShapefileDataStoreFactory.URLP.key, uploadedFile.toURI().toURL());
                params.put(ShapefileDataStoreFactory.NAMESPACEP.key, "http://www.opengis.net/gml");
                params.put(ShapefileDataStoreFactory.CREATE_SPATIAL_INDEX.key, Boolean.FALSE);
                params.put(ShapefileDataStoreFactory.ENABLE_SPATIAL_INDEX.key, Boolean.FALSE);
                params.put(ShapefileDataStoreFactory.MEMORY_MAPPED.key, Boolean.FALSE);
                params.put(ShapefileDataStoreFactory.DBFCHARSET.key, charset);
                dataStore = factory.createDataStore(params);
                checkParameter(dataStore != null, "Unable to open '%s' as a shapefile",
                        uploadedFile.getName());

                return dataStore;
            } catch (IOException e) {
                throw new CommandFailedException("Error opening shapefile: " + e.getMessage(), e);
            }

        }
    }
}
