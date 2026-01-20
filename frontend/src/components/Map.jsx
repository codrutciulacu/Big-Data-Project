import { useState } from 'react';
import { MapContainer, TileLayer, Popup, CircleMarker } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';

// Coordonate București
const BUCHAREST_CENTER = [44.4268, 26.1025];

function Map({ routes = [], topPriority = [] }) {
  const [filters, setFilters] = useState({
    critic: true,
    aglomerat: true,
    mediu: true,
    normal: true,
    faraDate: true
  });

  if (!routes || routes.length === 0) {
    return <div>Loading routes...</div>;
  }

  // Creăm map cu KPI și suggestions pentru fiecare rută
  const routeDataMap = {};
  topPriority.forEach(item => {
    const kpi = item.kpi || {};
    const suggestion = item.suggestion || {};
    
    // Calculăm load factor (pasageri observați împărțit la capacitate standard autobuz ~80)
    const loadFactor = kpi.observed_passengers ? kpi.observed_passengers / 80 : 0;
    
    routeDataMap[item.route_id] = {
      loadFactor: loadFactor,
      predictedPassengers: kpi.predicted_passengers || 0,
      observedPassengers: kpi.observed_passengers || 0,
      priorityScore: suggestion.priority_score || 0,
      action: suggestion.action || 'keep',
      message: suggestion.message || ''
    };
  });

  // Funcție de colorare bazată pe load factor (demand real)
  // Roșu: load factor > 25 (foarte aglomerat, >2000 pasageri)
  // Portocaliu: load factor 15-25 (aglomerat, 1200-2000 pasageri)
  // Galben: load factor 8-15 (mediu, 640-1200 pasageri)
  // Verde: load factor < 8 (normal, <640 pasageri)
  const getColorByDemand = (routeId) => {
    const data = routeDataMap[routeId];
    if (!data) return '#9ca3af'; // gri pentru rute fără date
    
    const lf = data.loadFactor;
    if (lf > 25) return '#ef4444';      // roșu - crictic
    if (lf > 15) return '#f97316';      // portocaliu - aglomerat
    if (lf > 8) return '#eab308';       // galben - mediu
    return '#10b981';                    // verde - normal
  };

  const getCategory = (routeId) => {
    const data = routeDataMap[routeId];
    if (!data) return 'faraDate';
    
    const lf = data.loadFactor;
    if (lf > 25) return 'critic';
    if (lf > 15) return 'aglomerat';
    if (lf > 8) return 'mediu';
    return 'normal';
  };

  const toggleFilter = (category) => {
    setFilters(prev => ({ ...prev, [category]: !prev[category] }));
  };

  const filteredRoutes = routes.filter(route => filters[getCategory(route.route_id)]);

  console.log(`Showing ${filteredRoutes.length}/${routes.length} routes on map. Data available for ${Object.keys(routeDataMap).length} routes.`);

  return (
    <div style={{ position: 'relative', height: '100%', width: '100%' }}>
      <MapContainer 
        center={BUCHAREST_CENTER} 
        zoom={12} 
        style={{ height: '100%', width: '100%' }}
      >
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        
        {/* Afișăm toate rutele cu culori bazate pe demand */}
        {filteredRoutes.map((route) => {
        if (!route.stops || route.stops.length === 0) return null;
        
        const color = getColorByDemand(route.route_id);
        const data = routeDataMap[route.route_id];
        
        return (
          <div key={route.route_id}>
            {/* Stopurile cu culori după demand */}
            {route.stops.map((stop) => (
              <CircleMarker
                key={`${route.route_id}-${stop.stop_id}`}
                center={[stop.lat, stop.lon]}
                radius={6}
                pathOptions={{ 
                  color: '#ffffff',
                  fillColor: color,
                  fillOpacity: 0.8,
                  weight: 2
                }}
              >
                <Popup>
                  <div style={{ minWidth: '200px' }}>
                    <strong>Linia {route.route_short_name}</strong><br/>
                    <small>{route.route_long_name}</small><br/>
                    <hr style={{margin: '8px 0'}} />
                    <strong>Stație:</strong> {stop.stop_name}<br/>
                    {data && (
                      <>
                        <hr style={{margin: '8px 0'}} />
                        <strong>Pasageri observați:</strong> {data.observedPassengers.toFixed(0)}<br/>
                        <strong>Pasageri previzionați:</strong> {data.predictedPassengers.toFixed(0)}<br/>
                        <strong>Load Factor:</strong> {data.loadFactor.toFixed(1)}x<br/>
                        <strong>Acțiune:</strong> <span style={{
                          padding: '2px 6px',
                          borderRadius: '4px',
                          fontSize: '11px',
                          backgroundColor: data.action === 'increase_frequency' ? '#fee2e2' : '#dbeafe',
                          color: data.action === 'increase_frequency' ? '#dc2626' : '#2563eb'
                        }}>
                          {data.action.replace(/_/g, ' ')}
                        </span><br/>
                        <small style={{color: '#6b7280', marginTop: '4px', display: 'block'}}>
                          {data.message}
                        </small>
                      </>
                    )}
                  </div>
                </Popup>
              </CircleMarker>
            ))}
          </div>
        );
      })}
      </MapContainer>

      {/* Panel Filtre */}
      <div style={{
        position: 'absolute',
        top: '20px',
        left: '70px',
        backgroundColor: 'white',
        padding: '12px 16px',
        borderRadius: '8px',
        boxShadow: '0 2px 8px rgba(0,0,0,0.15)',
        zIndex: 1000,
        fontSize: '13px',
        minWidth: '220px'
      }}>
        <div style={{ fontWeight: 600, marginBottom: '10px', color: '#1f2937', display: 'flex', alignItems: 'center', gap: '6px' }}>
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <path d="M22 3H2l8 9.46V19l4 2v-8.54L22 3z"/>
          </svg>
          Filtre Rute
        </div>
        
        <label style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px', cursor: 'pointer' }}>
          <input 
            type="checkbox" 
            checked={filters.critic}
            onChange={() => toggleFilter('critic')}
            style={{ cursor: 'pointer', accentColor: '#10b981' }}
          />
          <div style={{ 
            width: '14px', 
            height: '14px', 
            borderRadius: '50%', 
            backgroundColor: '#ef4444',
            border: '2px solid white',
            boxShadow: '0 0 0 1px #e5e7eb'
          }}></div>
          <span style={{ color: '#4b5563' }}>Critic</span>
        </label>

        <label style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px', cursor: 'pointer' }}>
          <input 
            type="checkbox" 
            checked={filters.aglomerat}
            onChange={() => toggleFilter('aglomerat')}
            style={{ cursor: 'pointer', accentColor: '#10b981' }}
          />
          <div style={{ 
            width: '14px', 
            height: '14px', 
            borderRadius: '50%', 
            backgroundColor: '#f97316',
            border: '2px solid white',
            boxShadow: '0 0 0 1px #e5e7eb'
          }}></div>
          <span style={{ color: '#4b5563' }}>Aglomerat</span>
        </label>

        <label style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px', cursor: 'pointer' }}>
          <input 
            type="checkbox" 
            checked={filters.mediu}
            onChange={() => toggleFilter('mediu')}
            style={{ cursor: 'pointer', accentColor: '#10b981' }}
          />
          <div style={{ 
            width: '14px', 
            height: '14px', 
            borderRadius: '50%', 
            backgroundColor: '#eab308',
            border: '2px solid white',
            boxShadow: '0 0 0 1px #e5e7eb'
          }}></div>
          <span style={{ color: '#4b5563' }}>Mediu</span>
        </label>

        <label style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px', cursor: 'pointer' }}>
          <input 
            type="checkbox" 
            checked={filters.normal}
            onChange={() => toggleFilter('normal')}
            style={{ cursor: 'pointer', accentColor: '#10b981' }}
          />
          <div style={{ 
            width: '14px', 
            height: '14px', 
            borderRadius: '50%', 
            backgroundColor: '#10b981',
            border: '2px solid white',
            boxShadow: '0 0 0 1px #e5e7eb'
          }}></div>
          <span style={{ color: '#4b5563' }}>Normal</span>
        </label>

        <label style={{ display: 'flex', alignItems: 'center', gap: '8px', cursor: 'pointer' }}>
          <input 
            type="checkbox" 
            checked={filters.faraDate}
            onChange={() => toggleFilter('faraDate')}
            style={{ cursor: 'pointer', accentColor: '#10b981' }}
          />
          <div style={{ 
            width: '14px', 
            height: '14px', 
            borderRadius: '50%', 
            backgroundColor: '#9ca3af',
            border: '2px solid white',
            boxShadow: '0 0 0 1px #e5e7eb'
          }}></div>
          <span style={{ color: '#4b5563' }}>Fără date</span>
        </label>
      </div>

      {/* Legendă */}
      <div style={{
        position: 'absolute',
        bottom: '20px',
        right: '20px',
        backgroundColor: 'white',
        padding: '12px 16px',
        borderRadius: '8px',
        boxShadow: '0 2px 8px rgba(0,0,0,0.15)',
        zIndex: 1000,
        fontSize: '13px',
        minWidth: '200px'
      }}>
        <div style={{ fontWeight: 600, marginBottom: '8px', color: '#1f2937' }}>
          Nivel de Aglomerare
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '6px' }}>
          <div style={{ 
            width: '16px', 
            height: '16px', 
            borderRadius: '50%', 
            backgroundColor: '#ef4444',
            border: '2px solid white',
            boxShadow: '0 0 0 1px #e5e7eb'
          }}></div>
          <span style={{ color: '#4b5563' }}>Critic (&gt;2000 pas/oră)</span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '6px' }}>
          <div style={{ 
            width: '16px', 
            height: '16px', 
            borderRadius: '50%', 
            backgroundColor: '#f97316',
            border: '2px solid white',
            boxShadow: '0 0 0 1px #e5e7eb'
          }}></div>
          <span style={{ color: '#4b5563' }}>Aglomerat (1200-2000)</span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '6px' }}>
          <div style={{ 
            width: '16px', 
            height: '16px', 
            borderRadius: '50%', 
            backgroundColor: '#eab308',
            border: '2px solid white',
            boxShadow: '0 0 0 1px #e5e7eb'
          }}></div>
          <span style={{ color: '#4b5563' }}>Mediu (640-1200)</span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '6px' }}>
          <div style={{ 
            width: '16px', 
            height: '16px', 
            borderRadius: '50%', 
            backgroundColor: '#10b981',
            border: '2px solid white',
            boxShadow: '0 0 0 1px #e5e7eb'
          }}></div>
          <span style={{ color: '#4b5563' }}>Normal (&lt;640)</span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <div style={{ 
            width: '16px', 
            height: '16px', 
            borderRadius: '50%', 
            backgroundColor: '#9ca3af',
            border: '2px solid white',
            boxShadow: '0 0 0 1px #e5e7eb'
          }}></div>
          <span style={{ color: '#4b5563' }}>Fără date</span>
        </div>
      </div>
    </div>
  );
}

export default Map;
