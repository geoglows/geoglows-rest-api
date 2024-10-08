require(["esri/Map", "esri/views/MapView"], (Map, MapView) => {
    const map = new Map({
        basemap: "topo-vector"
    });
    const view = new MapView({
        container: "viewDiv", // Reference to the view div created in step 5
        map: map, // Reference to the map object created before the view
        zoom: 4, // Sets zoom level based on level of detail (LOD)
        center: [15, 65] // Sets center point of view using longitude,latitude
    });
});