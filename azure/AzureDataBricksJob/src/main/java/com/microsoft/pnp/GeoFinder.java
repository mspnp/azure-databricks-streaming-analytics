package com.microsoft.pnp;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.geotools.data.FeatureSource;
import org.geotools.data.collection.SpatialIndexFeatureCollection;
import org.geotools.data.collection.SpatialIndexFeatureSource;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.store.ReprojectingFeatureCollection;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.referencing.CRS;
import org.opengis.feature.Feature;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.expression.PropertyName;
import org.opengis.referencing.FactoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


public class GeoFinder implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(GeoFinder.class);

    private final FeatureSource featureSource;
    private final FilterFactory2 filterFactory;
    private final PropertyName propertyName;
    private final GeometryFactory geometryFactory;

    private GeoFinder(FeatureSource featureSource, FilterFactory2 filterFactory, PropertyName propertyName) {
        this.featureSource = featureSource;
        this.filterFactory = filterFactory;
        this.propertyName = propertyName;
        this.geometryFactory = new GeometryFactory();
    }

    public Optional<String> getNeighborhood(double longitude, double latitude) {
        logger.debug(String.format("Searching for coordinate (%f, %f)", longitude, latitude));
        Point point = this.geometryFactory.createPoint(new Coordinate(longitude, latitude));
        Filter filter = this.filterFactory.contains(propertyName, filterFactory.literal(point));
        try {
            FeatureCollection featureCollection = this.featureSource.getFeatures(filter);
            try (FeatureIterator iterator = featureCollection.features()) {
                if (iterator.hasNext()) {
                    Feature feature = iterator.next();
                    return Optional.of(feature.getProperty("Name").getValue().toString());
                }
            }
        } catch (IOException ex) {

            logger.warn(String.format("Error searching for coordinate (%f, %f)", longitude, latitude), ex);
        }

        return Optional.of("Unknown");
    }

    public static GeoFinder createGeoFinder(URL shapeFileUrl) throws IOException {
        try {
            logger.info(String.format("Using shapefile: %s", shapeFileUrl));
            ShapefileDataStore dataStore = new ShapefileDataStore(shapeFileUrl);
            String[] typeNames = dataStore.getTypeNames();
            String typeName = typeNames[0];

            logger.info(String.format("Reading content %s", typeName));
            ReprojectingFeatureCollection features = new ReprojectingFeatureCollection(
                    dataStore.getFeatureSource(typeName).getFeatures(),
                    CRS.decode("epsg:4269"),
                    CRS.decode("epsg:4326")
            );
            FeatureSource featureSource = new SpatialIndexFeatureSource(
                    new SpatialIndexFeatureCollection(features));

            FilterFactory2 filterFactory = CommonFactoryFinder.getFilterFactory2();
            PropertyName propertyName = filterFactory.property(dataStore
                    .getSchema(typeName)
                    .getGeometryDescriptor()
                    .getName());
            return new GeoFinder(featureSource, filterFactory, propertyName);
        } catch (IOException ioe) {
            logger.error(String.format("Error loading Geospatial data from %s", shapeFileUrl), ioe);
            throw ioe;
        } catch (FactoryException fe) {
            logger.error(String.format("Error loading Geographic Coordinate System"), fe);
            throw new IOException(fe);
        }
    }
}
