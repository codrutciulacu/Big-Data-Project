import { useState, useEffect, useCallback } from 'react';

export function useOptimizationData() {
    const [summary, setSummary] = useState(null);
    const [topPriority, setTopPriority] = useState([]);
    const [routes, setRoutes] = useState([]);
    const [peakHours, setPeakHours] = useState([]);
    const [loading, setLoading] = useState(true);

    const fetchData = useCallback(() => {
        setLoading(true);
        // Citim JSON-urile din folderul exports cu timestamp pentru a evita cache
        const timestamp = new Date().getTime();
        Promise.all([
            fetch(`/exports/summary_latest.json?t=${timestamp}`).then(r => r.json()),
            fetch(`/exports/top_priority_latest.json?t=${timestamp}`).then(r => r.json()),
            fetch(`/exports/routes_geo_latest.json?t=${timestamp}`).then(r => r.json()),
            fetch(`/exports/peak_hours_latest.json?t=${timestamp}`).then(r => r.json()),
        ])
            .then(([summaryData, topPriorityData, routesData, peakHoursData]) => {
                setSummary(summaryData);
                setTopPriority(topPriorityData);
                setRoutes(routesData);
                setPeakHours(peakHoursData);
                setLoading(false);
            })
            .catch(err => {
                console.error('Error loading data:', err);
                setLoading(false);
            });
    }, []);

    useEffect(() => {
        fetchData();
    }, [fetchData]);

    return { summary, topPriority, routes, peakHours, loading, refresh: fetchData };
}
