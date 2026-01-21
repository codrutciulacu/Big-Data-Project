import { useOptimizationData } from '../hooks/useOptimizationData';
import './Dashboard.css';

function Dashboard() {
  const { summary, topPriority, loading } = useOptimizationData();

  console.log('Dashboard data:', { summary, topPriority, loading });

  if (loading) {
    return <div className="dashboard">Loading data...</div>;
  }

  if (!summary) {
    return <div className="dashboard">No data available</div>;
  }

  return (
    <div className="dashboard">
      <div className="summary-cards">
        <div className="card">
          <h3>Rute Totale</h3>
          <p className="big-number">{summary.total_routes}</p>
        </div>
        <div className="card">
          <h3>Sugestii</h3>
          <p className="big-number">{summary.suggestions}</p>
        </div>
        <div className="card">
          <h3>Date Necesare</h3>
          <p className="big-number">{summary.needs_data}</p>
        </div>
      </div>

      <div className="actions-summary">
        <h3>Distribuția Acțiunilor</h3>
        {summary.actions && Object.entries(summary.actions).map(([action, count]) => (
          <div key={action} className="action-item">
            <span className="label">{action.replace(/_/g, ' ')}</span>
            <span className="count">{count}</span>
          </div>
        ))}
      </div>

      <div className="top-priority">
        <h3>Rute cu Prioritate Ridicată</h3>
        <table>
          <thead>
            <tr>
              <th>Rută</th>
              <th>Oră</th>
              <th>Acțiune</th>
              <th>Scor Prioritate</th>
              <th>Mesaj</th>
            </tr>
          </thead>
          <tbody>
            {topPriority.slice(0, 10).map((route, idx) => {
              const suggestion = route.suggestion || {};
              const hour = route.hour_ts ? new Date(route.hour_ts).toLocaleTimeString('ro-RO', { hour: '2-digit', minute: '2-digit' }) : 'N/A';
              return (
                <tr key={idx}>
                  <td><strong>{route.route_id || 'N/A'}</strong></td>
                  <td>{hour}</td>
                  <td>
                    <span className={`badge ${suggestion.action || 'unknown'}`}>
                      {suggestion.action ? suggestion.action.replace(/_/g, ' ') : 'N/A'}
                    </span>
                  </td>
                  <td>{suggestion.priority_score?.toFixed(2) || 'N/A'}</td>
                  <td>{suggestion.message || 'Fără mesaj'}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default Dashboard;
