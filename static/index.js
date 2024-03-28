// Function to display the chart
function clearChart() {
    const domElement = document.getElementById('tvchart');
    domElement.innerHTML = ''; // Clear the content of the chart container
}
async function displayChart() {
    const fileInput = document.getElementById('fileInput');
    const files = fileInput.files;

    if (files.length === 0) {
        console.log('Please select a file');
        return;
    }

    const file = files[0]; // Retrieve the first file
    const interval = document.getElementById('timeFrame').value.trim();

    
    
    try {
       

        // Fetch CSRF token from the cookie
        const csrfToken = getCookie('csrftoken');

        // Create FormData object to send file
        console.log('Interval:', interval);

        const formData = new FormData();
        formData.append('file', file); // Append the file to FormData
        formData.append('interval', interval); // Append the interval
        
        // Fetch stock data from Django backend
        const response = await fetch('/core/get_stock_data_view/', {
            method: 'POST',
            headers: {
                'X-CSRFToken': csrfToken
            },
            body: formData // Send FormData instead of JSON
        });

        if (!response.ok) {
            throw new Error('Failed to fetch stock data');
        }
        const stockData = await response.json();

        // Log the stock data
        console.log(typeof stockData);

        console.log('Stock Data:', stockData);

        // Display the stock chart
        const chartProperties = {
            width: 1500,
            height: 700,
            timeScale: {
                timeVisible: true,
                secondsVisible: true,
            },
        };
        clearChart();
        const domElement = document.getElementById('tvchart');
        const chart = LightweightCharts.createChart(domElement, chartProperties);
        
        // Add candlestick series
        const candleseries = chart.addCandlestickSeries();

        // Prepare data for the chart
        const klinedata = stockData.map((data) => ({
            time: data.Date,
            open: (data.Open)*1,
            high: (data.High)*1,
            low: (data.Low)*1,
            close: (data.Close)*1,
            volume: (data.Volume)*1,
            sma: (data.MA30)*1 // Assuming the column name for MA30 is unchanged
        }));
        console.log(klinedata);
        candleseries.setData(klinedata);

        // Add SMA series
        const sma_series = chart.addLineSeries({ color: '#e89a09', lineWidth: 1 });
        const sma_data = klinedata
            .filter((d) => d.sma)
            .map((d) => ({ time: d.time, value: d.sma }));
        sma_series.setData(sma_data);
    } catch (error) {
        console.log('Error:', error);
    }
}

// Function to get cookie by name
function getCookie(name) {
    const value = `; ${document.cookie}`;
    const parts = value.split(`; ${name}=`);
    if (parts.length === 2) return parts.pop().split(';').shift();
}

// Call displayChart function when the page loads
displayChart();
