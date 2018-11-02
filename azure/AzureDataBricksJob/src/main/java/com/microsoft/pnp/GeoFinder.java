package com.microsoft.pnp;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.geotools.data.FeatureSource;
import org.geotools.data.collection.SpatialIndexFeatureCollection;
import org.geotools.data.collection.SpatialIndexFeatureSource;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.Feature;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.expression.PropertyName;
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

    private FeatureSource featureSource;
    private FilterFactory2 filterFactory;
    private PropertyName propertyName;
    private GeometryFactory geometryFactory;

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
            FeatureIterator iterator = featureCollection.features();
            try {
                if (iterator.hasNext()) {
                    Feature feature = iterator.next();
                    return Optional.of(feature.getProperty("Name").getValue().toString());
                }
            } finally {
                iterator.close();
            }
        } catch (IOException ex) {

            logger.warn(String.format("Error searching for coordinate (%f, %f)", longitude, latitude), ex);
        }

        return Optional.of("Unknown");
    }

    public static GeoFinder createGeoFinder(URL shapeFileUrl) throws IOException {
        try {
            logger.info(String.format("Using shapefile: %s", shapeFileUrl));
            Map<String, String> connect = new HashMap<>();
            connect.put("url", shapeFileUrl.toString());
            ShapefileDataStore dataStore = new ShapefileDataStore(shapeFileUrl);
            String[] typeNames = dataStore.getTypeNames();
            String typeName = typeNames[0];

            logger.info(String.format("Reading content %s", typeName));
            FeatureSource featureSource = new SpatialIndexFeatureSource(
                    new SpatialIndexFeatureCollection(dataStore.getFeatureSource(typeName).getFeatures()));

            FilterFactory2 filterFactory = CommonFactoryFinder.getFilterFactory2();
            PropertyName propertyName = filterFactory.property(dataStore
                    .getSchema(typeName)
                    .getGeometryDescriptor()
                    .getName());
            return new GeoFinder(featureSource, filterFactory, propertyName);
        } catch (IOException ex) {
            logger.error(String.format("Error loading Geospatial data from %s", shapeFileUrl));
            throw ex;
        }
    }
}
