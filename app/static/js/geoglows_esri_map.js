require([
    "esri/Map",
    "esri/views/MapView",
    "esri/layers/MapImageLayer",
    "esri/TimeExtent",
    "esri/widgets/TimeSlider",
    "esri/layers/GraphicsLayer",
    "esri/Graphic"
], function(Map, MapView, MapImageLayer, TimeExtent, TimeSlider, GraphicsLayer, Graphic) {

    ////////////////////////////////////////////////////////////////////////  ESRI LAYER ANIMATION CONTROLS
    let layerAnimationTime = new Date();
    layerAnimationTime.setUTCHours(0, 0, 0, 0);
    const currentDate = $("#current-map-date");
    const startDateTime = new Date(layerAnimationTime);
    const endDateTime = new Date(layerAnimationTime.setUTCHours(5 * 24));
    layerAnimationTime = new Date(startDateTime);
    currentDate.html(layerAnimationTime);

    ////////////////////////////////////////////////////////////////////////  MAKE THE MAP
    const geoglowsURL = "https://livefeeds3.arcgis.com/arcgis/rest/services/GEOGLOWS/GlobalWaterModel_Medium/MapServer";
    const globalLayer = new MapImageLayer({
        url: geoglowsURL,
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
    const selectedSegmentLayer = new GraphicsLayer();
    const markerLayer = new GraphicsLayer();
    
    const map = new Map({
        basemap: "topo-vector",
        layers: [globalLayer, selectedSegmentLayer, markerLayer]
    });
    
    const view = new MapView({
        container: "map",
        map: map,
        center: [0, 20],
        zoom: 2
    });

    ////////////////////////////////////////////////////////////////////////  ESRI LAYER ANIMATION CONTROLS
    const slider = new TimeSlider({
        container: "time-slider",
        mode: "cumulative-from-start",
        view: view,
        timeVisible: true,
        loop: true,
        playRate: 3000  // miliseconds
    })

    view.whenLayerView(globalLayer).then((lv) => {
        slider.fullTimeExtent = globalLayer.timeInfo.fullTimeExtent.expandTo("hours");
        slider.stops = {
            interval: {
                unit: "hours",
                value: 12
            }  // globalLayer.timeInfo.interval
        };
    });

    ////////////////////////////////////////////////////////////////////////  MAKE THE MAP

    let river_id;
    let forecastsLoaded = false;

    const endpoint = "https://geoglows.ecmwf.int/api/v2/"
    const fc = $("#forecast-chart");
    const hc = $("#historical-chart");
    
    view.ui.add(document.getElementById('show-modal-btn'), "bottom-right");
    view.on("click", mapClickEvent);

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
                Plotly.newPlot('forecast-chart', traces, {
                    title: 'Forecasted Flow<br>River ID: ' + river_id,
                    xaxis: {title: 'Date (UTC +00:00)'},
                    yaxis: {title: 'Discharge (m³/s)'}
                });
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
        if (view.zoom < 16) {
            view.goTo({target: event.mapPoint, zoom: 16});
            return
        } else {
            view.goTo({target: event.mapPoint});
        }
        markerLayer.removeAll();
        let markPath = "M12 2C8.13 2 5 5.13 5 9c0 5.25 7 13 7 13s7-7.75 7-13c0-3.87-3.13-7-7-7zm0 9c-1.1 0-2-.9-2-2s.9-2 2-2 2 .9 2 2-.9 2-2 2z";
        let marker = new Graphic({
            geometry: event.mapPoint,
            symbol: {
                type: "simple-marker",
                color: "#007bff",
                size: "40px",
                yoffset: 15,
                outline: {
                    color: "white",
                    width: 1
                },
                path: markPath,
            }
        })
        markerLayer.add(marker);
        $("#chart_modal").modal('show');
        updateStatusIcons('identify');

        globalLayer
            .findSublayerById(0)
            .queryFeatures({
                geometry: event.mapPoint,
                distance: 125,
                units: "meters",
                spatialRelationship: "intersects",
                outFields: ["*"],
                returnGeometry: true,
            })
            .then(response => {
                if (!response.features.length) {
                    alert("Error finding the rivier!");
                    updateStatusIcons('fail');
                    return;
                }
                river_id = response.features[0].attributes.comid
                if (river_id === "Null" || !river_id) {
                    alert("Error finding the rivier!");
                    updateStatusIcons('fail');
                    return;
                }
                selectedSegmentLayer.removeAll();
                let segment = new Graphic({
                    geometry: response.features[0].geometry,
                    symbol: {
                        type: "simple-line",
                        color: "#00008b",
                        width: 3
                    }
                });
                selectedSegmentLayer.add(segment);

                clearChartDivs();
                updateStatusIcons('load');
                updateDownloadLinks('clear');
                plotForecastStats(river_id);
            })
    }
});
