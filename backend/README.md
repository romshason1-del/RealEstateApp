# StreetIQ Property Value API (Express)

Standalone Express server for property value insights. Uses only official Israeli government data (data.gov.il).

## Run

From project root:

```bash
npm run backend
```

Or from backend folder (after `npm install` in project root):

```bash
cd backend && npm start
```

## Endpoints

- `GET /api/property-value?city=...&street=...&houseNumber=...` – Property value insights
- `GET /health` – Health check

## Example

```
GET http://localhost:3001/api/property-value?city=תל%20אביב&street=דיזנגוף&houseNumber=10
```
