////////////////////////////////////////////////////////////////////////  ESRI LAYER ANIMATION CONTROLS
let layerAnimationTime = new Date();
layerAnimationTime = new Date(layerAnimationTime.toISOString())
layerAnimationTime.setUTCHours(0);
layerAnimationTime.setUTCMinutes(0);
layerAnimationTime.setUTCSeconds(0);
layerAnimationTime.setUTCMilliseconds(0);
const currentDate = $("#current-map-date");
const startDateTime = new Date(layerAnimationTime);
const endDateTime = new Date(layerAnimationTime.setUTCHours(5 * 24));
layerAnimationTime = new Date(startDateTime);
currentDate.html(layerAnimationTime);
const slider = $("#time-slider");
slider.change(function () {
    refreshLayerAnimation()
})

function refreshLayerAnimation() {
    layerAnimationTime = new Date(startDateTime);
    layerAnimationTime.setUTCHours(slider.val() * 3);
    currentDate.html(layerAnimationTime);
    globalLayer.setTimeRange(layerAnimationTime, endDateTime);
}

let animate = false;

function playAnimation(once = false) {
    if (!animate) {
        return
    }
    if (layerAnimationTime < endDateTime) {
        slider.val(Number(slider.val()) + 1)
    } else {
        slider.val(0)
    }
    refreshLayerAnimation();
    if (once) {
        animate = false;
        return
    }
    setTimeout(playAnimation, 750);
}

$("#animationPlay").click(function () {
    animate = true;
    playAnimation()
})
$("#animationStop").click(function () {
    animate = false;
})
$("#animationPlus1").click(function () {
    animate = true;
    playAnimation(true)
})
$("#animationBack1").click(function () {
    if (layerAnimationTime > startDateTime) {
        slider.val(Number(slider.val()) - 1)
    } else {
        slider.val(40)
    }
    refreshLayerAnimation();
})
const map = L.map('map', {
    zoom: 2,
    minZoom: 2,
    boxZoom: true,
    maxBounds: L.latLngBounds(L.latLng(-100, -225), L.latLng(100, 225)),
    center: [20, 0],
});
const basemap = L.esri.basemapLayer('Topographic').addTo(map);
const globalLayer = L.esri.dynamicMapLayer({
    url: 'https://livefeeds2.arcgis.com/arcgis/rest/services/GEOGLOWS/GlobalWaterModel_Medium/MapServer',
    useCors: false,
    layers: [0],
    from: startDateTime,
    to: endDateTime,
}).addTo(map);