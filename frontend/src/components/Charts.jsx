import { LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { useOptimizationData } from '../hooks/useOptimizationData';
import './Charts.css';

function Charts() {
  const { summary, topPriority, peakHours } = useOptimizationData();

  // Pregătim datele pentru graficul de evoluție pasageri pe oră
  const passengerData = peakHours?.map(item => ({
    ora: `${item.hour_of_day}:00`,
    previzionat: Math.round(item.avg_predicted_passengers || 0),
    observat: Math.round(item.avg_observed_passengers || 0),
  })) || [];

  // Pregătim datele pentru graficul de distribuție acțiuni
  const actionsData = summary?.actions ? Object.entries(summary.actions).map(([action, count]) => ({
    actiune: action.replace('increase_frequency', 'Crește frecvența')
               .replace('decrease_frequency', 'Scade frecvența')
               .replace('keep', 'Menține'),
    numar: count
  })) : [];

  // Pregătim datele pentru top 10 rute după scor prioritate
  const topRoutesData = topPriority?.slice(0, 10).map(route => ({
    ruta: `Ruta ${route.route_id}`,
    scor: route.suggestion?.priority_score || 0,
  })) || [];

  return (
    <div className="charts">
      <div className="chart-grid">
        
        {/* Grafic 1: Evoluția pasagerilor pe oră */}
        <div className="chart-card">
          <div className="chart-header">
            <h3>Evoluția Pasagerilor pe Oră</h3>
            <p>Comparație între pasageri previzionați și observați</p>
          </div>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={passengerData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis 
                dataKey="ora" 
                stroke="#6b7280"
                style={{ fontSize: '0.875rem' }}
              />
              <YAxis 
                stroke="#6b7280"
                style={{ fontSize: '0.875rem' }}
              />
              <Tooltip 
                contentStyle={{ 
                  backgroundColor: 'white', 
                  border: '1px solid #e5e7eb',
                  borderRadius: '0.5rem',
                  fontSize: '0.875rem'
                }}
              />
              <Legend 
                wrapperStyle={{ fontSize: '0.875rem' }}
              />
              <Line 
                type="monotone" 
                dataKey="previzionat" 
                stroke="#10b981" 
                strokeWidth={2}
                name="Previzionat"
                dot={{ fill: '#10b981', r: 4 }}
              />
              <Line 
                type="monotone" 
                dataKey="observat" 
                stroke="#3b82f6" 
                strokeWidth={2}
                name="Observat"
                dot={{ fill: '#3b82f6', r: 4 }}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Grafic 2: Distribuția acțiunilor recomandate */}
        <div className="chart-card">
          <div className="chart-header">
            <h3>Distribuția Acțiunilor</h3>
            <p>Număr de sugestii per tip de acțiune</p>
          </div>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={actionsData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis 
                dataKey="actiune" 
                stroke="#6b7280"
                style={{ fontSize: '0.875rem' }}
              />
              <YAxis 
                stroke="#6b7280"
                style={{ fontSize: '0.875rem' }}
              />
              <Tooltip 
                contentStyle={{ 
                  backgroundColor: 'white', 
                  border: '1px solid #e5e7eb',
                  borderRadius: '0.5rem',
                  fontSize: '0.875rem'
                }}
              />
              <Bar 
                dataKey="numar" 
                fill="#10b981"
                radius={[8, 8, 0, 0]}
                name="Număr sugestii"
              />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Grafic 3: Top 10 rute după scor prioritate */}
        <div className="chart-card full-width">
          <div className="chart-header">
            <h3>Top 10 Rute după Scor Prioritate</h3>
            <p>Rutele cu cea mai mare prioritate de optimizare</p>
          </div>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={topRoutesData} layout="vertical">
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis 
                type="number"
                stroke="#6b7280"
                style={{ fontSize: '0.875rem' }}
              />
              <YAxis 
                type="category"
                dataKey="ruta" 
                stroke="#6b7280"
                style={{ fontSize: '0.875rem' }}
                width={80}
              />
              <Tooltip 
                contentStyle={{ 
                  backgroundColor: 'white', 
                  border: '1px solid #e5e7eb',
                  borderRadius: '0.5rem',
                  fontSize: '0.875rem'
                }}
              />
              <Bar 
                dataKey="scor" 
                fill="#10b981"
                radius={[0, 8, 8, 0]}
                name="Scor prioritate"
              />
            </BarChart>
          </ResponsiveContainer>
        </div>

      </div>
    </div>
  );
}

export default Charts;
