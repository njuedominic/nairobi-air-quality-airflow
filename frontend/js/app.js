const API_BASE_URL = window.API_BASE_URL || "http://127.0.0.1:8000";
const mapElement = document.getElementById("map");
let runtimeConfig;

try {
    const response = await fetch(`${API_BASE_URL}/config`);
    if (!response.ok) {
        throw new Error(`Configuration request failed: ${response.status}`);
    }
    runtimeConfig = await response.json();
} catch (error) {
    mapElement.textContent = "Map configuration unavailable.";
    throw error;
}

const MAPBOX_TOKEN = runtimeConfig.mapbox_access_token;
if (!MAPBOX_TOKEN) {
    mapElement.textContent = "Mapbox access token is not configured.";
    throw new Error("Set MAPBOX_ACCESS_TOKEN in the API environment.");
}
mapboxgl.accessToken = MAPBOX_TOKEN;

const PARAMETER_CONFIG = {
    pm25: {
        label: "PM2.5",
        unit: "µg/m³",
        valueKey: "latest_value",
        legend: [
            { color: "#2DC937", text: "0–9.9 µg/m³" },
            { color: "#E7B416", text: "10–24.9 µg/m³" },
            { color: "#DB7B2B", text: "25–49.9 µg/m³" },
            { color: "#CC3232", text: "50+ µg/m³" }
        ]
    },
    pm1: {
        label: "PM1",
        unit: "µg/m³",
        valueKey: "latest_value",
        legend: [
            { color: "#2DC937", text: "0–9.9 µg/m³" },
            { color: "#E7B416", text: "10–24.9 µg/m³" },
            { color: "#DB7B2B", text: "25–49.9 µg/m³" },
            { color: "#CC3232", text: "50+ µg/m³" }
        ]
    },
    temperature: {
        label: "Temperature",
        unit: "°C",
        valueKey: "latest_value",
        legend: [
            { color: "#3B82F6", text: "< 15°C" },
            { color: "#2DC937", text: "15–25°C" },
            { color: "#E7B416", text: "25–35°C" },
            { color: "#CC3232", text: "> 35°C" }
        ]
    },
    relativehumidity: {
        label: "Relative Humidity",
        unit: "%",
        valueKey: "latest_value",
        legend: [
            { color: "#E7B416", text: "< 30%" },
            { color: "#2DC937", text: "30–60%" },
            { color: "#3B82F6", text: "60–80%" },
            { color: "#6366F1", text: "> 80%" }
        ]
    },
    pm10: {
        label: "PM10",
        unit: "µg/m³",
        valueKey: "latest_value",
        legend: [
            { color: "#2DC937", text: "0–19 µg/m³" },
            { color: "#E7B416", text: "20–49 µg/m³" },
            { color: "#DB7B2B", text: "50–99 µg/m³" },
            { color: "#CC3232", text: "100+ µg/m³" }
        ]
    },
    no2: {
        label: "NO2",
        unit: "µg/m³",
        valueKey: "latest_value",
        legend: [
            { color: "#2DC937", text: "0–20 µg/m³" },
            { color: "#E7B416", text: "20–40 µg/m³" },
            { color: "#DB7B2B", text: "40–80 µg/m³" },
            { color: "#CC3232", text: "80+ µg/m³" }
        ]
    },
    o3: {
        label: "O3",
        unit: "µg/m³",
        valueKey: "latest_value",
        legend: [
            { color: "#2DC937", text: "0–30 µg/m³" },
            { color: "#E7B416", text: "30–60 µg/m³" },
            { color: "#DB7B2B", text: "60–120 µg/m³" },
            { color: "#CC3232", text: "120+ µg/m³" }
        ]
    },
    co: {
        label: "CO",
        unit: "ppm",
        valueKey: "latest_value",
        legend: [
            { color: "#2DC937", text: "0–1 ppm" },
            { color: "#E7B416", text: "1–2 ppm" },
            { color: "#DB7B2B", text: "2–5 ppm" },
            { color: "#CC3232", text: "5+ ppm" }
        ]
    }
};

function buildAirQualityUrl(parameter) {
    return `${API_BASE_URL}/air-quality?parameter=${encodeURIComponent(parameter)}`;
}

function getColorExpressionForParameter(parameter) {
    const config = PARAMETER_CONFIG[parameter] || PARAMETER_CONFIG.pm25;

    if (parameter === "temperature") {
        return [
            "case",
            ["==", ["get", config.valueKey], null], "#6B7280",
            ["step", ["to-number", ["get", config.valueKey]], "#3B82F6", 15, "#2DC937", 25, "#E7B416", 35, "#CC3232"]
        ];
    }

    if (parameter === "relativehumidity") {
        return [
            "case",
            ["==", ["get", config.valueKey], null], "#6B7280",
            ["step", ["to-number", ["get", config.valueKey]], "#E7B416", 30, "#2DC937", 60, "#3B82F6", 80, "#6366F1"]
        ];
    }

    return [
        "case",
        ["==", ["get", config.valueKey], null], "#6B7280",
        ["step", ["to-number", ["get", config.valueKey]], "#2DC937", 10, "#E7B416", 25, "#DB7B2B", 50, "#CC3232"]
    ];
}

function updateLegend(parameter) {
    const config = PARAMETER_CONFIG[parameter] || PARAMETER_CONFIG.pm25;
    const heading = document.querySelector("#legend h4");
    const itemsContainer = document.getElementById("legend-items");

    heading.textContent = config.label;
    itemsContainer.innerHTML = config.legend
        .map((item) => `
            <div class="legend-row">
                <span class="legend-dot" style="background:${item.color};"></span>
                <span>${item.text}</span>
            </div>
        `)
        .join("");
}

const map = new mapboxgl.Map({
    container: "map",
    style: "mapbox://styles/mapbox/standard",
    config: {
        basemap: {
            lightPreset: "night",
            showPointOfInterestLabels: false,
            showRoadLabels: false,
            showTransitLabels: false,
            showPlaceLabels: true,
            showPedestrianRoads: false,
            show3dObjects: false
        }
    },
    center: [36.817223, -1.286389],
    zoom: 12
});

function setupParameterFilter() {
    const parameterSelect = document.getElementById("parameter-select");
    parameterSelect.addEventListener("change", (event) => {
        const parameter = event.target.value;
        const source = map.getSource("air-quality");

        updateLegend(parameter);
        map.setPaintProperty("air-quality-points", "circle-color", getColorExpressionForParameter(parameter));

        if (source) {
            source.setData(buildAirQualityUrl(parameter));
        }
    });
}

map.on("load", () => {
    const initialParameter = document.getElementById("parameter-select").value;

    map.addSource("air-quality", {
        type: "geojson",
        data: buildAirQualityUrl(initialParameter)
    });

    map.addLayer({
        id: "air-quality-points",
        type: "circle",
        source: "air-quality",
        paint: {
            "circle-radius": [
                "interpolate",
                ["linear"],
                ["get", "latest_value"],
                0, 6,
                10, 8,
                25, 10,
                50, 12,
                100, 14
            ],
            "circle-color": getColorExpressionForParameter(initialParameter),
            "circle-opacity": 0.9,
            "circle-stroke-width": 2,
            "circle-stroke-color": "#ffffff",
            "circle-emissive-strength": 1
        }
    });

    updateLegend(initialParameter);
    setupParameterFilter();
});

map.on("click", "air-quality-points", (event) => {
    const feature = event.features[0];
    const properties = feature.properties;
    const coordinates = feature.geometry.coordinates.slice();

    const popupHtml = `
        <div>
            <h3>${properties.location_name}</h3>
            <p><strong>Parameter:</strong> ${properties.parameter}</p>
            <p><strong>Latest value:</strong> ${properties.latest_value ?? "N/A"} ${properties.unit || ""}</p>
            <p><strong>24h average:</strong> ${properties.avg_24h ?? "N/A"} ${properties.unit || ""}</p>
            <p><strong>Status:</strong> ${properties.data_status}</p>
            <p><strong>Last measurement:</strong> ${properties.latest_measurement_at}</p>
        </div>
    `;

    new mapboxgl.Popup()
        .setLngLat(coordinates)
        .setHTML(popupHtml)
        .addTo(map);
});

map.on("mouseenter", "air-quality-points", () => {
    map.getCanvas().style.cursor = "pointer";
});

map.on("mouseleave", "air-quality-points", () => {
    map.getCanvas().style.cursor = "";
});
