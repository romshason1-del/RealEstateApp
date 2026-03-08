export type MockPropertyInsight = {
  estimatedPropertyValue: string;
  lastTransactions: string[];
};

const propertyInsights: MockPropertyInsight[] = [
  {
    estimatedPropertyValue: "$1,125,000",
    lastTransactions: [
      "$1,030,000 in Mar 2024",
      "$945,000 in Nov 2022",
      "$880,000 in Jul 2020",
    ],
  },
  {
    estimatedPropertyValue: "$1,280,000",
    lastTransactions: [
      "$1,190,000 in Jan 2024",
      "$1,040,000 in Sep 2022",
      "$965,000 in Jun 2020",
    ],
  },
  {
    estimatedPropertyValue: "$987,000",
    lastTransactions: [
      "$910,000 in Feb 2024",
      "$845,000 in Oct 2022",
      "$790,000 in May 2020",
    ],
  },
  {
    estimatedPropertyValue: "$1,410,000",
    lastTransactions: [
      "$1,290,000 in Apr 2024",
      "$1,155,000 in Dec 2022",
      "$1,010,000 in Aug 2020",
    ],
  },
];

export function getMockPropertyInsight(seed: number): MockPropertyInsight {
  return propertyInsights[Math.abs(seed) % propertyInsights.length];
}
