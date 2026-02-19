// path: ui-react/src/data/airports.ts
// Airport coordinates for route visualization
// Synchronized with scripts/generate_fraud_scenario.py

export interface Airport {
  lat: number;
  lon: number;
  country: string;
  city: string;
}

export const AIRPORTS: Record<string, Airport> = {
  // ============================================================
  // Europe
  // ============================================================

  // France
  CDG: { lat: 49.0097, lon: 2.5479, country: 'FR', city: 'Paris CDG' },
  ORY: { lat: 48.7262, lon: 2.3652, country: 'FR', city: 'Paris ORY' },
  BVA: { lat: 49.4546, lon: 2.1128, country: 'FR', city: 'Beauvais' },
  NCE: { lat: 43.6653, lon: 7.2150, country: 'FR', city: 'Nice' },
  MRS: { lat: 43.4367, lon: 5.2150, country: 'FR', city: 'Marseille' },
  LYS: { lat: 45.7256, lon: 5.0811, country: 'FR', city: 'Lyon' },
  TLS: { lat: 43.6293, lon: 1.3632, country: 'FR', city: 'Toulouse' },
  BOD: { lat: 44.8283, lon: -0.7156, country: 'FR', city: 'Bordeaux' },
  NTE: { lat: 47.1532, lon: -1.6107, country: 'FR', city: 'Nantes' },
  LIL: { lat: 50.5633, lon: 3.0869, country: 'FR', city: 'Lille' },
  RNS: { lat: 48.0695, lon: -1.7348, country: 'FR', city: 'Rennes' },
  BIQ: { lat: 43.4683, lon: -1.5311, country: 'FR', city: 'Biarritz' },
  AJA: { lat: 41.9236, lon: 8.8029, country: 'FR', city: 'Ajaccio' },
  BIA: { lat: 42.5527, lon: 9.4837, country: 'FR', city: 'Bastia' },
  FDF: { lat: 14.5908, lon: -61.0032, country: 'FR', city: 'Fort-de-France' },
  PTP: { lat: 16.2653, lon: -61.5318, country: 'FR', city: 'Pointe-à-Pitre' },
  RUN: { lat: -20.8900, lon: 55.5160, country: 'FR', city: 'Réunion' },
  PPT: { lat: -17.5567, lon: -149.6117, country: 'FR', city: 'Tahiti' },

  // United Kingdom
  LHR: { lat: 51.4700, lon: -0.4543, country: 'GB', city: 'London Heathrow' },
  LGW: { lat: 51.1537, lon: -0.1821, country: 'GB', city: 'London Gatwick' },
  LCY: { lat: 51.5053, lon: 0.0553, country: 'GB', city: 'London City' },
  STN: { lat: 51.8840, lon: 0.2350, country: 'GB', city: 'London Stansted' },
  LTN: { lat: 51.8747, lon: -0.3683, country: 'GB', city: 'London Luton' },
  MAN: { lat: 53.3650, lon: -2.2720, country: 'GB', city: 'Manchester' },
  EDI: { lat: 55.9500, lon: -3.3725, country: 'GB', city: 'Edinburgh' },
  GLA: { lat: 55.8719, lon: -4.4331, country: 'GB', city: 'Glasgow' },
  BHX: { lat: 52.4539, lon: -1.7480, country: 'GB', city: 'Birmingham' },
  BRS: { lat: 51.3827, lon: -2.7191, country: 'GB', city: 'Bristol' },
  NCL: { lat: 55.0375, lon: -1.6917, country: 'GB', city: 'Newcastle' },
  BFS: { lat: 54.6575, lon: -6.2158, country: 'GB', city: 'Belfast' },

  // Germany
  FRA: { lat: 50.0379, lon: 8.5622, country: 'DE', city: 'Frankfurt' },
  MUC: { lat: 48.3538, lon: 11.7861, country: 'DE', city: 'Munich' },
  TXL: { lat: 52.5597, lon: 13.2877, country: 'DE', city: 'Berlin Tegel' },
  BER: { lat: 52.3667, lon: 13.5033, country: 'DE', city: 'Berlin' },
  DUS: { lat: 51.2895, lon: 6.7668, country: 'DE', city: 'Düsseldorf' },
  HAM: { lat: 53.6300, lon: 9.9882, country: 'DE', city: 'Hamburg' },
  CGN: { lat: 50.8659, lon: 7.1427, country: 'DE', city: 'Cologne' },
  STR: { lat: 48.6900, lon: 9.2219, country: 'DE', city: 'Stuttgart' },
  HAJ: { lat: 52.4611, lon: 9.6850, country: 'DE', city: 'Hannover' },
  NUE: { lat: 49.4987, lon: 11.0669, country: 'DE', city: 'Nuremberg' },

  // Spain & Portugal
  MAD: { lat: 40.4719, lon: -3.5626, country: 'ES', city: 'Madrid' },
  BCN: { lat: 41.2974, lon: 2.0833, country: 'ES', city: 'Barcelona' },
  AGP: { lat: 36.6749, lon: -4.4991, country: 'ES', city: 'Málaga' },
  VLC: { lat: 39.4893, lon: -0.4816, country: 'ES', city: 'Valencia' },
  PMI: { lat: 39.5517, lon: 2.7388, country: 'ES', city: 'Palma' },
  TFS: { lat: 28.0445, lon: -16.5725, country: 'ES', city: 'Tenerife South' },
  LPA: { lat: 27.9319, lon: -15.3866, country: 'ES', city: 'Gran Canaria' },
  OPO: { lat: 41.2481, lon: -8.6814, country: 'PT', city: 'Porto' },
  LIS: { lat: 38.7742, lon: -9.1342, country: 'PT', city: 'Lisbon' },
  FAO: { lat: 37.0144, lon: -7.9659, country: 'PT', city: 'Faro' },
  FNC: { lat: 32.6979, lon: -16.7745, country: 'PT', city: 'Madeira' },

  // Italy
  FCO: { lat: 41.8003, lon: 12.2389, country: 'IT', city: 'Rome Fiumicino' },
  CIA: { lat: 41.7990, lon: 12.5949, country: 'IT', city: 'Rome Ciampino' },
  MXP: { lat: 45.6306, lon: 8.7281, country: 'IT', city: 'Milan Malpensa' },
  LIN: { lat: 45.4451, lon: 9.2767, country: 'IT', city: 'Milan Linate' },
  BGY: { lat: 45.6739, lon: 9.7042, country: 'IT', city: 'Bergamo' },
  VCE: { lat: 45.5053, lon: 12.3519, country: 'IT', city: 'Venice' },
  NAP: { lat: 40.8860, lon: 14.2908, country: 'IT', city: 'Naples' },
  FLR: { lat: 43.8099, lon: 11.2051, country: 'IT', city: 'Florence' },
  PSA: { lat: 43.6828, lon: 10.3956, country: 'IT', city: 'Pisa' },
  CAG: { lat: 39.2515, lon: 9.0568, country: 'IT', city: 'Cagliari' },

  // Netherlands, Belgium, Switzerland, Austria
  AMS: { lat: 52.3105, lon: 4.7683, country: 'NL', city: 'Amsterdam' },
  BRU: { lat: 50.9010, lon: 4.4844, country: 'BE', city: 'Brussels' },
  GVA: { lat: 46.2381, lon: 6.1090, country: 'CH', city: 'Geneva' },
  ZRH: { lat: 47.4647, lon: 8.5492, country: 'CH', city: 'Zürich' },
  BSL: { lat: 47.5900, lon: 7.5292, country: 'CH', city: 'Basel' },
  VIE: { lat: 48.1103, lon: 16.5697, country: 'AT', city: 'Vienna' },

  // Nordics
  CPH: { lat: 55.6180, lon: 12.6560, country: 'DK', city: 'Copenhagen' },
  ARN: { lat: 59.6519, lon: 17.9186, country: 'SE', city: 'Stockholm Arlanda' },
  BMA: { lat: 59.3544, lon: 17.9417, country: 'SE', city: 'Stockholm Bromma' },
  OSL: { lat: 60.1939, lon: 11.1004, country: 'NO', city: 'Oslo' },
  HEL: { lat: 60.3172, lon: 24.9633, country: 'FI', city: 'Helsinki' },
  KEF: { lat: 63.9850, lon: -22.6056, country: 'IS', city: 'Reykjavik' },

  // Eastern Europe
  WAW: { lat: 52.1657, lon: 20.9671, country: 'PL', city: 'Warsaw' },
  KRK: { lat: 50.0777, lon: 19.7848, country: 'PL', city: 'Krakow' },
  PRG: { lat: 50.1008, lon: 14.2600, country: 'CZ', city: 'Prague' },
  BUD: { lat: 47.4330, lon: 19.2610, country: 'HU', city: 'Budapest' },
  OTP: { lat: 44.5711, lon: 26.0850, country: 'RO', city: 'Bucharest' },
  SOF: { lat: 42.6967, lon: 23.4114, country: 'BG', city: 'Sofia' },
  ATH: { lat: 37.9364, lon: 23.9445, country: 'GR', city: 'Athens' },
  SKG: { lat: 40.5197, lon: 22.9709, country: 'GR', city: 'Thessaloniki' },
  RIX: { lat: 56.9221, lon: 23.9798, country: 'LV', city: 'Riga' },
  VNO: { lat: 54.6341, lon: 25.2858, country: 'LT', city: 'Vilnius' },
  TLL: { lat: 59.4133, lon: 24.8328, country: 'EE', city: 'Tallinn' },

  // ============================================================
  // Middle East & Turkey
  // ============================================================

  DXB: { lat: 25.2532, lon: 55.3657, country: 'AE', city: 'Dubai' },
  DWC: { lat: 24.8963, lon: 55.1614, country: 'AE', city: 'Dubai World Central' },
  AUH: { lat: 24.4330, lon: 54.6511, country: 'AE', city: 'Abu Dhabi' },
  DOH: { lat: 25.2731, lon: 51.6081, country: 'QA', city: 'Doha' },
  BAH: { lat: 26.2700, lon: 50.6336, country: 'BH', city: 'Bahrain' },
  MCT: { lat: 23.5933, lon: 58.2844, country: 'OM', city: 'Muscat' },
  IST: { lat: 41.2753, lon: 28.7519, country: 'TR', city: 'Istanbul' },
  SAW: { lat: 40.8986, lon: 29.3092, country: 'TR', city: 'Istanbul Sabiha' },
  ESB: { lat: 40.1149, lon: 32.9931, country: 'TR', city: 'Ankara' },
  ADB: { lat: 38.2924, lon: 27.1569, country: 'TR', city: 'Izmir' },
  AMM: { lat: 31.7226, lon: 35.9932, country: 'JO', city: 'Amman' },
  RUH: { lat: 24.9578, lon: 46.6989, country: 'SA', city: 'Riyadh' },
  JED: { lat: 21.6796, lon: 39.1565, country: 'SA', city: 'Jeddah' },
  TLV: { lat: 32.0055, lon: 34.8854, country: 'IL', city: 'Tel Aviv' },

  // ============================================================
  // North America
  // ============================================================

  // United States
  JFK: { lat: 40.6413, lon: -73.7781, country: 'US', city: 'New York JFK' },
  EWR: { lat: 40.6895, lon: -74.1745, country: 'US', city: 'Newark' },
  LGA: { lat: 40.7769, lon: -73.8740, country: 'US', city: 'New York LaGuardia' },
  BOS: { lat: 42.3656, lon: -71.0096, country: 'US', city: 'Boston' },
  PHL: { lat: 39.8744, lon: -75.2424, country: 'US', city: 'Philadelphia' },
  IAD: { lat: 38.9531, lon: -77.4565, country: 'US', city: 'Washington Dulles' },
  DCA: { lat: 38.8521, lon: -77.0377, country: 'US', city: 'Washington National' },
  ATL: { lat: 33.6407, lon: -84.4277, country: 'US', city: 'Atlanta' },
  CLT: { lat: 35.2140, lon: -80.9431, country: 'US', city: 'Charlotte' },
  MIA: { lat: 25.7959, lon: -80.2870, country: 'US', city: 'Miami' },
  FLL: { lat: 26.0726, lon: -80.1527, country: 'US', city: 'Fort Lauderdale' },
  MCO: { lat: 28.4312, lon: -81.3081, country: 'US', city: 'Orlando' },
  TPA: { lat: 27.9755, lon: -82.5332, country: 'US', city: 'Tampa' },
  ORD: { lat: 41.9742, lon: -87.9073, country: 'US', city: 'Chicago O\'Hare' },
  MDW: { lat: 41.7868, lon: -87.7522, country: 'US', city: 'Chicago Midway' },
  DFW: { lat: 32.8998, lon: -97.0403, country: 'US', city: 'Dallas/Fort Worth' },
  IAH: { lat: 29.9902, lon: -95.3368, country: 'US', city: 'Houston' },
  HOU: { lat: 29.6454, lon: -95.2789, country: 'US', city: 'Houston Hobby' },
  DEN: { lat: 39.8561, lon: -104.6737, country: 'US', city: 'Denver' },
  PHX: { lat: 33.4342, lon: -112.0116, country: 'US', city: 'Phoenix' },
  LAS: { lat: 36.0840, lon: -115.1537, country: 'US', city: 'Las Vegas' },
  LAX: { lat: 33.9416, lon: -118.4085, country: 'US', city: 'Los Angeles' },
  SNA: { lat: 33.6757, lon: -117.8682, country: 'US', city: 'Orange County' },
  SAN: { lat: 32.7338, lon: -117.1933, country: 'US', city: 'San Diego' },
  SFO: { lat: 37.6213, lon: -122.3790, country: 'US', city: 'San Francisco' },
  SJC: { lat: 37.3639, lon: -121.9289, country: 'US', city: 'San Jose' },
  OAK: { lat: 37.7126, lon: -122.2197, country: 'US', city: 'Oakland' },
  SEA: { lat: 47.4502, lon: -122.3088, country: 'US', city: 'Seattle' },
  PDX: { lat: 45.5898, lon: -122.5951, country: 'US', city: 'Portland' },
  MSP: { lat: 44.8830, lon: -93.2223, country: 'US', city: 'Minneapolis' },
  DTW: { lat: 42.2162, lon: -83.3554, country: 'US', city: 'Detroit' },
  CVG: { lat: 39.0550, lon: -84.6613, country: 'US', city: 'Cincinnati' },
  BNA: { lat: 36.1263, lon: -86.6774, country: 'US', city: 'Nashville' },
  SLC: { lat: 40.7899, lon: -111.9791, country: 'US', city: 'Salt Lake City' },
  CWA: { lat: 44.7776, lon: -89.6668, country: 'US', city: 'Wausau' },

  // Canada
  YYZ: { lat: 43.6777, lon: -79.6248, country: 'CA', city: 'Toronto' },
  YUL: { lat: 45.4706, lon: -73.7408, country: 'CA', city: 'Montreal' },
  YOW: { lat: 45.3225, lon: -75.6692, country: 'CA', city: 'Ottawa' },
  YVR:  { lat: 49.1947, lon: -123.1792, country: 'CA', city: 'Vancouver' },
  YYC: { lat: 51.1220, lon: -114.0120, country: 'CA', city: 'Calgary' },
  YEG: { lat: 53.3097, lon: -113.5807, country: 'CA', city: 'Edmonton' },

  // ============================================================
  // Latin America & Caribbean
  // ============================================================

  // Mexico & Central America
  MEX: { lat: 19.4361, lon: -99.0719, country: 'MX', city: 'Mexico City' },
  CUN: { lat:  21.0365, lon: -86.8771, country: 'MX', city: 'Cancún' },
  GDL: { lat: 20.5218, lon: -103.3112, country: 'MX', city: 'Guadalajara' },
  MTY: { lat: 25.7785, lon: -100.1070, country: 'MX', city: 'Monterrey' },
  SJO: { lat: 9.9982, lon: -84.2041, country: 'CR', city: 'San José CR' },
  PTY: { lat: 9.0669, lon: -79.3876, country: 'PA', city: 'Panama City' },

  // Caribbean
  SDQ: { lat: 18.4297, lon: -69.6689, country: 'DO', city: 'Santo Domingo' },
  PUJ: { lat: 18.5674, lon: -68.3634, country: 'DO', city: 'Punta Cana' },
  SJU: { lat: 18.4394, lon: -66.0018, country: 'US', city: 'San Juan PR' },
  HAV: { lat: 22.9891, lon: -82.4091, country: 'CU', city: 'Havana' },

  // South America
  GRU: { lat: -23.4356, lon: -46.4731, country: 'BR', city: 'São Paulo GRU' },
  CGH: { lat: -23.6261, lon: -46.6566, country: 'BR', city: 'São Paulo CGH' },
  GIG: { lat: -22.8090, lon: -43.2506, country: 'BR', city: 'Rio de Janeiro GIG' },
  SDU: { lat: -22.9105, lon: -43.1631, country: 'BR', city: 'Rio de Janeiro SDU' },
  BSB: { lat: -15.8697, lon: -47.9208, country: 'BR', city: 'Brasília' },
  EZE: { lat: -34.8138, lon: -58.5390, country: 'AR', city: 'Buenos Aires EZE' },
  AEP: { lat: -34.5592, lon: -58.4156, country: 'AR', city: 'Buenos Aires AEP' },
  SCL: { lat: -33.3930, lon: -70.7858, country: 'CL', city: 'Santiago' },
  LIM: { lat: -12.0219, lon: -77.1144, country: 'PE', city: 'Lima' },
  BOG: { lat: 4.7016, lon: -74.1469, country: 'CO', city: 'Bogotá' },
  UIO: { lat: -0.1292, lon: -78.3575, country: 'EC', city: 'Quito' },

  // ============================================================
  // Africa
  // ============================================================

  // North Africa
  CMN: { lat: 33.3675, lon: -7.5898, country: 'MA', city: 'Casablanca' },
  RAK: { lat: 31.6069, lon: -8.0363, country: 'MA', city: 'Marrakech' },
  TUN: { lat: 36.8510, lon: 10.2272, country: 'TN', city: 'Tunis' },
  ALG: { lat: 36.6910, lon: 3.2154, country: 'DZ', city: 'Algiers' },
  CAI: { lat: 30.1219, lon: 31.4056, country: 'EG', city: 'Cairo' },
  HRG: { lat: 27.1803, lon: 33.7984, country: 'EG', city: 'Hurghada' },
  SSH: { lat: 27.9773, lon: 34.3949, country: 'EG', city: 'Sharm el-Sheikh' },

  // Sub-Saharan Africa
  ADD: { lat: 8.9779, lon: 38.7993, country: 'ET', city: 'Addis Ababa' },
  NBO: { lat: -1.3192, lon: 36.9278, country: 'KE', city: 'Nairobi' },
  JNB: { lat: -26.1367, lon: 28.2411, country: 'ZA', city: 'Johannesburg' },
  CPT: { lat: -33.9715, lon: 18.6021, country: 'ZA', city: 'Cape Town' },
  LOS: { lat: 6.5774, lon: 3.3212, country: 'NG', city: 'Lagos' },
  ACC: { lat: 5.6052, lon: -0.1668, country: 'GH', city: 'Accra' },
  CKY: { lat: 9.5769, lon: -13.6120, country: 'GN', city: 'Conakry' },
  MRU: { lat: -20.4302, lon: 57.6836, country: 'MU', city: 'Mauritius' },
  SEZ: { lat: -4.6743, lon: 55.5218, country: 'SC', city: 'Seychelles' },

  // ============================================================
  // Asia
  // ============================================================

  // India & South Asia
  DEL: { lat: 28.5562, lon: 77.1000, country: 'IN', city: 'Delhi' },
  BOM: { lat: 19.0896, lon: 72.8656, country: 'IN', city: 'Mumbai' },
  BLR: { lat: 13.1989, lon: 77.7063, country: 'IN', city: 'Bangalore' },
  HYD: { lat: 17.2400, lon: 78.4289, country: 'IN', city: 'Hyderabad' },
  MAA: { lat: 12.9941, lon: 80.1709, country: 'IN', city: 'Chennai' },
  CCU: { lat: 22.6547, lon: 88.4467, country: 'IN', city: 'Kolkata' },
  COK: { lat: 10.1520, lon: 76.4019, country: 'IN', city: 'Kochi' },
  GOI: { lat: 15.3808, lon: 73.8314, country: 'IN', city: 'Goa' },

  // East Asia - Japan
  HND: { lat: 35.5494, lon: 139.7798, country: 'JP', city: 'Tokyo Haneda' },
  NRT: { lat: 35.7720, lon: 140.3929, country: 'JP', city: 'Tokyo Narita' },
  KIX: { lat: 34.4347, lon: 135.2440, country: 'JP', city: 'Osaka Kansai' },
  ITM: { lat: 34.7868, lon: 135.4382, country: 'JP', city: 'Osaka Itami' },
  NGO: { lat: 34.8584, lon: 136.8050, country: 'JP', city: 'Nagoya' },
  CTS: { lat: 42.7752, lon: 141.6923, country: 'JP', city: 'Sapporo' },

  // East Asia - Korea & China
  ICN: { lat: 37.4602, lon: 126.4407, country: 'KR', city: 'Seoul Incheon' },
  GMP: { lat: 37.5583, lon: 126.7946, country: 'KR', city: 'Seoul Gimpo' },
  PEK: { lat: 40.0799, lon: 116.6031, country: 'CN', city: 'Beijing Capital' },
  PKX: { lat: 39.5099, lon: 116.4108, country: 'CN', city: 'Beijing Daxing' },
  PVG: { lat: 31.1443, lon: 121.8083, country: 'CN', city: 'Shanghai Pudong' },
  SHA: { lat: 31.1960, lon: 121.3360, country: 'CN', city: 'Shanghai Hongqiao' },
  CAN: { lat: 23.3924, lon: 113.2990, country: 'CN', city: 'Guangzhou' },
  SZX: { lat: 22.6393, lon: 113.8110, country: 'CN', city: 'Shenzhen' },
  XMN: { lat: 24.5440, lon: 118.1280, country: 'CN', city: 'Xiamen' },
  HKG: { lat: 22.3080, lon: 113.9185, country: 'HK', city: 'Hong Kong' },
  TPE: { lat: 25.0797, lon: 121.2342, country: 'TW', city: 'Taipei' },
  TSA: { lat: 25.0694, lon: 121.5526, country: 'TW', city: 'Taipei Songshan' },

  // Southeast Asia
  BKK: { lat: 13.6900, lon: 100.7501, country: 'TH', city: 'Bangkok' },
  DMK: { lat: 13.9126, lon: 100.6067, country: 'TH', city: 'Bangkok Don Mueang' },
  SIN: { lat: 1.3644, lon: 103.9915, country: 'SG', city: 'Singapore' },
  KUL: { lat: 2.7456, lon: 101.7100, country: 'MY', city: 'Kuala Lumpur' },
  CGK: { lat: -6.1256, lon: 106.6559, country: 'ID', city: 'Jakarta' },
  DPS: { lat: -8.7482, lon: 115.1670, country: 'ID', city: 'Bali' },
  MNL: { lat: 14.5086, lon: 121.0190, country: 'PH', city: 'Manila' },
  HAN: { lat: 21.2141, lon: 105.8020, country: 'VN', city: 'Hanoi' },
  SGN: { lat: 10.8188, lon: 106.6520, country: 'VN', city: 'Ho Chi Minh City' },

  // ============================================================
  // Oceania
  // ============================================================

  // Australia
  SYD: { lat: -33.9399, lon: 151.1753, country: 'AU', city: 'Sydney' },
  MEL: { lat: -37.6733, lon: 144.8430, country: 'AU', city: 'Melbourne' },
  BNE: { lat: -27.3842, lon: 153.1175, country: 'AU', city: 'Brisbane' },
  PER: { lat: -31.9403, lon: 115.9670, country: 'AU', city: 'Perth' },
  ADL: { lat: -34.9440, lon: 138.5340, country: 'AU', city: 'Adelaide' },

  // New Zealand
  AKL: { lat: -37.0082, lon: 174.7910, country: 'NZ', city: 'Auckland' },
  WLG: { lat: -41.3272, lon: 174.8050, country: 'NZ', city: 'Wellington' },
  CHC: { lat: -43.4894, lon: 172.5320, country: 'NZ', city: 'Christchurch' },

  // Indian Ocean Islands
  MLE: { lat: 4.1918, lon: 73.5291, country: 'MV', city: 'Maldives' },
  CMB: { lat: 7.1739, lon: 79.8840, country: 'LK', city: 'Colombo' },
};

// Helper function to get airport coordinates
export function getAirportCoords(iataCode:  string): { lat: number; lng: number } | null {
  const airport = AIRPORTS[iataCode?. toUpperCase()?.trim()];
  if (airport) {
    return { lat: airport.lat, lng: airport.lon };
  }
  return null;
}

// Helper to check if code is valid
export function isValidIATA(code: string): boolean {
  return code?. length === 3 && AIRPORTS[code?.toUpperCase()] !== undefined;
}

// Get airport city name
export function getAirportCity(iataCode: string): string {
  const airport = AIRPORTS[iataCode?.toUpperCase()];
  return airport?.city || iataCode;
}

// Get all airport codes
export function getAllAirportCodes(): string[] {
  return Object.keys(AIRPORTS).sort();
}

// Search airports by city or country
export function searchAirports(query: string): Array<{ code: string; airport: Airport }> {
  const q = query.toLowerCase();
  return Object.entries(AIRPORTS)
    .filter(([code, airport]) =>
      code.toLowerCase().includes(q) ||
      airport.city.toLowerCase().includes(q) ||
      airport.country.toLowerCase().includes(q)
    )
    .map(([code, airport]) => ({ code, airport }));
}