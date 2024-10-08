require([
    "esri/Map",
    "esri/views/MapView",
    "esri/layers/MapImageLayer",
    "esri/TimeExtent",
    "esri/widgets/TimeSlider",
    "esri/layers/GraphicsLayer",
    "esri/Graphic",
    "esri/widgets/Expand"
], function(Map, MapView, MapImageLayer, TimeExtent, TimeSlider, GraphicsLayer, Graphic, Expand) {

    ////////////////////////////////////////////////////////////////////////  ESRI LAYER ANIMATION CONTROLS
    let layerAnimationTime = new Date();
    layerAnimationTime = new Date(layerAnimationTime.toISOString())
    layerAnimationTime.setUTCHours(0);
    layerAnimationTime.setUTCMinutes(0);
    layerAnimationTime.setUTCSeconds(0);
    layerAnimationTime.setUTCMilliseconds(0);  // TODO simplify the code
    const currentDate = $("#current-map-date");
    const startDateTime = new Date(layerAnimationTime);
    const endDateTime = new Date(layerAnimationTime.setUTCHours(5 * 24));
    layerAnimationTime = new Date(startDateTime);
    currentDate.html(layerAnimationTime);

    ////////////////////////////////////////////////////////////////////////  MAKE THE MAP
    const globalLayer = new MapImageLayer({
        url: 'https://livefeeds3.arcgis.com/arcgis/rest/services/GEOGLOWS/GlobalWaterModel_Medium/MapServer',
        sublayers: [
            {
                id: 0,
                visible: true,
                timeExtent: new TimeExtent({
                    start: startDateTime,
                    end: endDateTime
                })
            }
        ],
    });

    const selectedSegment = new GraphicsLayer();

    const map = new Map({
        basemap: "topo-vector",
        layers: [globalLayer, selectedSegment]
    });
    
    const view = new MapView({
        container: "map",
        map: map,
        zoom: 2, // TODO set the center and the boundbox
    });

    ////////////////////////////////////////////////////////////////////////  ESRI LAYER ANIMATION CONTROLS
    const slider = new TimeSlider({
        container: "time-slider",
        mode: "cumulative-from-start",
        view: view,
        timeVisible: true,
        loop: true
    })

    view.whenLayerView(globalLayer).then((lv) => {
        slider.fullTimeExtent = globalLayer.timeInfo.fullTimeExtent.expandTo("hours");
        slider.stops = {
            interval: globalLayer.timeInfo.interval
        };
    });

    //////////////////////////////////////////////////////////////////////////  FUNCTIONS FOR MAP INTERACTION
    function clearChartDivs() {
        fc.html('');
        hc.html('');
    }

    function updateStatusIcons(type) {
        let statusObj = $("#request-status");
        if (type === 'identify') {
            statusObj.html(' (Getting Stream ID)');
            statusObj.css('color', 'orange');
        } else if (type === 'load') {
            statusObj.html(' (Loading ID ' + river_id + ')');
            statusObj.css('color', 'orange');
        } else if (type === 'ready') {
            statusObj.html(' (Ready)');
            statusObj.css('color', 'green');
        } else if (type === 'fail') {
            statusObj.html(' (Failed)');
            statusObj.css('color', 'red');
        } else if (type === 'cleared') {
            statusObj.html(' (Cleared)');
            statusObj.css('color', 'grey');
        }
    }

    function updateDownloadLinks(type) {
        if (type === 'clear') {
            $("#download-forecast-btn").attr('href', '');
        } else if (type === 'set') {
            $("#download-forecast-btn").attr('href', endpoint + 'forecast/' + river_id);
        }
    }

    function plotForecastStats(id) {
        forecastsLoaded = false;
        $.ajax({
            type: 'GET',
            async: true,
            url: endpoint + 'forecast/' + id + '/?format=json',
            success: function (data) {
                fc.html('');
                let dt_rv = data['datetime'].concat(data['datetime'].slice().reverse());
                let traces = [
                    {
                        name: 'Uncertainty Bounds',
                        x: dt_rv,
                        y: data['flow_uncertainty_upper'].concat(data['flow_uncertainty_lower'].slice().reverse()),
                        mode: 'lines',
                        type: 'scatter',
                        fill: 'toself',
                        line: {color: 'lightblue', dash: 'none'},
                    },
                    {
                        name: 'Median Flow',
                        x: data['datetime'],
                        y: data['flow_median'],
                        mode: 'lines',
                        type: 'scatter',
                        fill: 'none',
                        line: {color: 'black'},
                    },
                ]
                Plotly.newPlot('forecast-chart', traces, {title: 'Forecasted Flow<br>River ID: ' + river_id});
                forecastsLoaded = true;
                updateStatusIcons('ready');
                updateDownloadLinks('set');
            },
            error: function () {
                console.log('forecast fail');
                updateStatusIcons('fail');
                clearChartDivs();
            }
        })
    }

    function mapClickEvent(event) {
        if (map.getZoom() < 16) {
            map.flyTo(event.latlng, 16);
            return
        } else {
            map.flyTo(event.latlng)
        }
        if (marker) {
            map.removeLayer(marker)
        }
        marker = L.marker(event.latlng).addTo(map);  // TODO switch to ArcGIS API
        $("#chart_modal").modal('show');
        updateStatusIcons('identify');
        L.esri.identifyFeatures({ 
            url: 'https://livefeeds3.arcgis.com/arcgis/rest/services/GEOGLOWS/GlobalWaterModel_Medium/MapServer'
        })
            .on(map)
            .at([event.latlng['lat'], event.latlng['lng']])
            .tolerance(10)  // map pixels to buffer search point
            .precision(6)  // decimals in the returned coordinate pairs
            .run(function (error, featureCollection) {
                if (error) {
                    updateStatusIcons('fail');
                    alert('Error finding the river_id');
                    return
                }
                selectedSegment.clearLayers();
                selectedSegment.addData(featureCollection.features[0].geometry)
                console.log(featureCollection.features[0].properties);
                river_id = featureCollection.features[0].properties["TDX Hydro Link Number"];
                clearChartDivs();
                updateStatusIcons('load');
                updateDownloadLinks('clear');
                plotForecastStats(river_id);
            })
    }

////////////////////////////////////////////////////////////////////////  MAKE THE MAP

    let marker = null;
    const segmentSymbol = {
        type: "simple-line",
        color: "#00008b",
        weight: 5
    };

    const endpoint = "https://geoglows.ecmwf.int/api/v2/"
    const fc = $("#forecast-chart");
    const hc = $("#historical-chart");
    const ftl = $("#forecast_tab_link");
    const htl = $("#historical_tab_link");
    let forecastsLoaded, historicalLoaded = false;
    let river_id;
    let show_modal_btn = document.getElementById('show_modal_btn');
    view.ui.add(show_modal_btn, "bottom-right");

    const mt = $("#modal_toggles");  // TODO what is this?
    mt.mouseover(function () {
        map.off('click')
    })
    mt.mouseout(function () {
        map.on("click", function (event) {
            mapClickEvent(event)
        })
    });
    map.on("click", function (event) {
        mapClickEvent(event)
    });
});
