document.addEventListener('DOMContentLoaded', function() {
    const stockListContainer = document.getElementById('stock-list');

    if (stockListContainer) {
        fetch('/api/stocks')
            .then(response => response.json())
            .then(data => {
                if (data.error) {
                    stockListContainer.innerHTML = `<p class="error">Error loading stocks: ${data.error}</p>`;
                    return;
                }
                if (data.length === 0) {
                    stockListContainer.innerHTML = '<p>No stocks found. You can populate the database from the admin panel.</p>';
                    return;
                }

                const table = document.createElement('table');
                table.className = 'stock-table';

                const thead = document.createElement('thead');
                thead.innerHTML = `
                    <tr>
                        <th>Symbol</th>
                        <th>Name</th>
                        <th>Sector</th>
                        <th>Industry</th>
                    </tr>
                `;
                table.appendChild(thead);

                const tbody = document.createElement('tbody');
                data.forEach(stock => {
                    const row = document.createElement('tr');
                    row.innerHTML = `
                        <td><a href="/stock/${stock.symbol}">${stock.symbol}</a></td>
                        <td>${stock.name}</td>
                        <td>${stock.sector}</td>
                        <td>${stock.industry}</td>
                    `;
                    tbody.appendChild(row);
                });
                table.appendChild(tbody);

                stockListContainer.appendChild(table);
            })
            .catch(error => {
                console.error('Error fetching stock data:', error);
                stockListContainer.innerHTML = '<p class="error">Could not fetch stock data. Is the server running?</p>';
            });
    }
});
