
class MarketSentiment {
  company_name string @description("The name of the company. Company name should be either ltd, inc, corp, llc, plc, ag, gmbh, sa, nv, oyj, ab, as or s.a.")
  company_ticker string @description("The stock ticker symbol of the company, if available.")
  sector string @description("The sector the company belongs to.")
  sentiment string @description("The sentiment of the market, e.g., Positive, Negative, Neutral.")
}

class MarketSentimentEntity {
  entities MarketSentiment[] @description("A list of market sentiment entities.")
}

// Create a function to extract the resume from a string.
function ExtractSentiment(article: string) -> MarketSentimentEntity {
  // Specify a client as provider/model-name
  // you can use custom LLM params with a custom client name from clients.baml like "client CustomHaiku"
  client CustomGemini2Flash // Set GOOGLE_API_KEY to use this client.
  
  prompt #"
    You are an expert financial analyst. Your task is to extract specific, named companies, their corresponding sectors, and the market sentiment towards them from the provided article.

    **Instructions:**
    1.  **Extract Specific Commercial Entities:** Identify and extract only the proper names of individual commercial entities (e.g., "Apple Inc.", "Microsoft", "Clinical Pathways Group"). This includes corporations, firms, groups, and other for-profit organizations.
    2.  **Exclude Generalizations:** Do NOT extract general, plural, or categorical references like "US firms," "companies," or "investors."
    3.  **Company Name Clues:** A company name can often be identified by a corporate suffix (e.g., 'ltd', 'inc', 'corp', 'SA', 'Group') or if it is a well-known commercial entity.
    4.  **Exclude Non-Commercial Entities:** Do not extract names of countries, continents, cities, states, stock exchanges, or other geographical, political, or non-commercial entities (e.g., 'United States', 'China', 'Hong Kong stock exchange').
    5.  **Determine Sector:** For each extracted company, identify its industry sector (e.g., Technology, Pharmaceutical, Research).
    6.  **Analyze Sentiment:** Determine the market sentiment (Positive, Negative, or Neutral) for each company based on the article's context.

    Only return entities that are explicitly named individual companies or commercial groups.
    {{ article }}

    {{ ctx.output_format }}

    {{ _.role('user') }}
  "#
}



// Test the function with a sample resume. Open the VSCode playground to run this.


test extract_market_sentiment {
  functions [ExtractSentiment]
  args {
    article #"
     Chipmakers are likely to benefit from the ‘One Big Beautiful Bill Act’ passed by the Senate on Tuesday, as they will be eligible for an investment tax credit that can incentivize the semiconductor business.

What Happened: According to the provisions of the tax cut bill proposed by President Donald Trump, the Big Beautiful Bill aims to temporarily increase an investment tax credit for semiconductor manufacturers till the end of 2026.
It would raise the tax credit to 30% of investments in manufacturing facilities, from the current 25%, providing stronger incentives for companies to expand domestic production.

Trump was initially critical of the Chips and Science Act, which granted awards worth $32.5417 billion and provided loans up to $5.85 billion to 32 companies, as per the Semiconductor Industry Association.

However, Trump set up a new entity, the United States Investment Accelerator, within the Commerce Department, which now oversees the CHIPS Act program, as established by an executive order signed by him.
In early March, Taiwan Semiconductor Manufacturing Co. Ltd. (TSM) revealed its plans to inject $100 billion into U.S. advanced semiconductor manufacturing, significantly expanding its ongoing $65 billion Arizona initiative under the Chips Act and bringing total investment to $165 billion.

Apple Inc. (AAPL) has committed over $500 billion to U.S. investments, focusing on AI, silicon engineering, and expanding advanced manufacturing.

Price Action: The SPDR S&P 500 ETF Trust (SPY) and Invesco QQQ Trust ETF (QQQ), which track the S&P 500 index and Nasdaq 100 index, respectively, edged higher on Wednesday. The SPY was up 0.45% at $620.45, while the QQQ advanced 0.70% to $550.80, according to Benzinga Pro data.


Sentiment in the crude oil market has shifted to expecting prices to decline, with the debate at the industry's biggest gathering in Asia more about timing than direction.
The topic of presentations and conversations at the annual APPEC conference is always dominated by the outlook for prices, and this week's event was no different.
What has shifted is market sentiment and the least-sighted animal this year was a bull in a sea of bearish participants.
While there are many factors that drive crude oil prices, the two that dominated APPEC were the decision by OPEC+ to keep unwinding production cuts and the risks to the global economy from the trade and geopolitical policies of U.S. President Donald Trump.
The first is a supply factor and points to the now widespread market expectation that OPEC+ will continue to add barrels back as regaining market share is now more of a priority than defending price levels.
The decision on September 7 by the eight members of OPEC+ undertaking voluntary production cuts to add back 137,000 barrels per day (bpd) of output from October was largely viewed as minor and unlikely to alter the market balance in itself.
But what is seen as significant is the expectation that the producer group, which includes top exporters Saudi Arabia and Russia, will continue to increase output into the first half of 2026 and completely unwind the 1.65 million bpd of cuts from April 2023, having already ended an additional 2.2 million bpd from November 2023.
It's unlikely that all of these barrels will find their way to the market, but with global demand growth forecasts coalescing at a maximum of 1 million bpd this year and next, it's still likely that OPEC+ will add back enough to swamp the increase in demand.
The likelihood that non-OPEC oil output will also rise, especially from the Americas outside of the United States, is tilting the market consensus heavily towards a supply overhang in coming months.
The demand side is also clouded with uncertainty, especially over the impact of the tariffs imposed by Trump on imports into the United States.
Estimates of the impact of lifting the average tariff on goods imports from just over 2% to around 18% vary, but the consensus was overwhelmingly that these will act as a drag on growth, while boosting U.S. inflation, but likely lowering it elsewhere.
HIDDEN BULLS?
It was not all doom and gloom at APPEC, with some bullish factors getting an airing.
But the bullish case relies on a few things going better than expected, and a few things getting worse.
On the better side of the ledger, the world economy has sailed through the Trump barrage largely unscathed, with consumer sentiment and spending holding up in developed economies and developing economies able to continue to attract investment and trade.
On the worse side, there would have to be heightened, and more successful, measures to reduce the flow of Russian crude to both India and China, the only major buyers left for oil sanctioned as part of efforts to end Moscow's war in Ukraine.

More effective measures against Iranian and Venezuelan oil could also have an impact, and there is the ongoing risk of a flare-up in Middle East tensions given the conflict between Israel and Hamas.
In some ways much of the risk premium in the crude market currently is largely dependent on what steps Trump decides to take.
This is somewhat ironic as the one thing the otherwise inconsistent and unpredictable U.S. leader has been consistent on is that he wants lower oil prices.
But it is caution over what he might do that is keeping a risk premium in the market, and helping to keep global benchmark Brent futures anchored around $65 a barrel.
Another unknown is what tactics China's refiners are likely to adopt in coming months.
They have been storing vast quantities of crude this year, with some estimates as high as 600,000 bpd, and their total storage is now estimated at between 1.2 billion and 1.4 billion barrels, which is well in excess of the 90 days of import cover recommended by the International Energy Agency.
China has been building stockpiles largely on the back of discounted Russian barrels and a lower price trend for other grades in the second quarter of this year.
But the Chinese may be swinging towards the view that oil should be priced more in a $50-$60 range currently, and may start to cut back on imports in order to encourage lower prices.
  "#
  }
}



test extract_market_sentiment_2 {
  functions [ExtractSentiment]
  args {
    article #"
     Trade tensions between the US and China have de-escalated in recent months. Still, as I speak with corporate leaders and government officials in each country, it is increasinglyclear that the economic relationship between the two nations has fundamentally changed. Geopolitics, security, and supply chain resilience are now shaping economic decisions in a manner that could be as profound as cost and efficiency once were.

Globalization is not going away. Comparative advantage will continue to play an important role in shaping trade patterns, and after decades of investment, a substantial decoupling is not practical. But it is no longer sufficient for a trade relationship to be driven solely by lowest-cost production, just-in-time inventory, and seamless, direct supply chains.

Instead, corporate leaders are weighing these advantages against questions that a decade ago would have barely registered: What are the national security implications? Am I overly exposing my operational platforms to cyber risk? How flexible are we in responding to unforeseen geopolitical tensions?

In practice, this means that the investments US firms now choose to make in China must be significantly more selective than in the past. The relationship between the two countries is developing into one of clear strategic interdependence.

This next phase of globalization is more complex and challenging. Multinationals that previously focused on efficiency now center their strategy on resilience and diversification. Companies are increasingly paying for the insurance policy of availability and scalability of supply, prioritizing security over cost. Inventories are deeper and less efficient. Costs are higher, as is geopolitical scrutiny.

This shift is visible in the numbers. China’s share of US imports has dropped from an all-time high of 22 per cent in 2017 to 13.4 per cent in 2024. At the same time, Vietnam’s share has doubled and Mexico is now America’s largest trading partner. While no single market can replicate China’s scale, companies are now building multi-node networks, often bringing their Chinese partners with them to new markets to buffer against disruption.

Though most of the current focus is on trade, capital flows are similarly fragmenting. Foreign direct investment flows into China have fallen by more than 90 per cent over the past four years to the lowest level in three decades. Western investors have retrenched, particularly in sensitive sectors like technology—partially driven by concerns about the Chinese economy, but more likely in order to hedge their geopolitical bets. In fact, most firms are now exploring a China for China strategy.

While capital flows into China have rebounded slightly this year, this has largely come from portfolio, rather than direct, investments—perhaps a response to greater monetary and fiscal stimulus and the increase in listings on the Hong Kong stock exchange.

At the same time, China’s domestic economy is itself advancing. It has rapidly climbed the advanced manufacturing curve, and it is clearly committed to cementing its position as a global leader in technology.

Still, FDI remains low. In my conversations with Chinese officials, I regularly hear a desire to attract more foreign capital. Policymakers should work towards this worthy goal, so long as it is done consistently with US national security interests.

On the outbound side, new Chinese investment in the US has dropped by more than 95 per cent from its peak in 2016. Capital is being rapidly redirected towards partners in south-east Asia, the Middle East and Latin America. Future trade negotiations should provide guidance on sectors of the US economy where Chinese investment is permissible.

Operating in China today means weighing many structural complexities that weren’t always part of the equation. Regulatory uncertainty, data localization mandates, national security reviews, and capital controls are all part of this calculus.

Capital is being deployed with more caution, with an emphasis on the short term. Firms are investing in optionality: diversifying footprints, building local compliance strategies, and planning for abrupt policy shifts.

This is the world of strategic interdependence. It’s not about undoing global connections—but redefining them around new variables. The firms that I see succeeding are those that develop more granular intelligence, more versatility, and a deep understanding of local conditions.

The US-China relationship remains the most important of our time. Companies will need to embrace a complexity and adapt to this new dynamic.
  "#
  }
}


test extract_market_sentiment_3 {
  functions [ExtractSentiment]
  args {
    article #"
    A heavy blow was dealt to OmniCure Health Ltd. today as the pharmaceutical giant announced that its highly anticipated Alzheimer's drug, "CogniClear," failed to meet its primary endpoints in its Phase 3 clinical trial. The news sent the company's shares tumbling by over 30%, wiping out nearly $20 billion in market value. The trial, which was conducted by the independent research organization Clinical Pathways Group, showed no statistically significant improvement in cognitive function compared to a placebo. This outcome raises serious questions about OmniCure's late-stage pipeline, which was heavily reliant on CogniClear's success.

The disappointment for OmniCure has, however, created a surge of speculative interest in its smaller rival, BioGenix SA. The biotech firm is developing its own Alzheimer's treatment, which uses a completely different biological pathway. While still in Phase 2, BioGenix's drug has shown promising early data. Following the OmniCure announcement, BioGenix SA's stock jumped 15%, with many on Wall Street now viewing the small firm as a potential, albeit risky, frontrunner in the race for an effective Alzheimer's treatment. An analyst from JPMorgan Chase commented, "While the failure of CogniClear is unfortunate for patients, it opens the door for novel approaches like the one BioGenix is pursuing. The entire investment community will be watching their next data release with extreme interest."
  "#
  }
}

test extract_market_sentiment_4 {
  functions [ExtractSentiment]
  args {
    article #"
A seismic shift is being felt in the electric vehicle industry this week following a landmark announcement from the relatively small tech firm, QuantumCharge Inc. The company revealed it has successfully developed a solid-state battery that doubles the range of current lithium-ion technology while cutting charging times by 80%. This breakthrough, if scalable, could solve the two biggest hurdles to mass EV adoption.

The news was not received as warmly by all. Industry giant Volta Motors, which has invested billions in its current battery production facilities, saw its stock dip by 8% in pre-market trading. While the company has not issued a formal statement, sources say executives are scrambling to assess the validity of QuantumCharge's claims. Analysts at Morgan Stanley noted, "Volta Motors faces a classic innovator's dilemma. Embracing this new tech would mean writing off massive existing investments, but ignoring it could risk long-term obsolescence."

Meanwhile, raw material suppliers are in a complex position. Lithium Dynamics Corp., a key supplier of refined lithium to both Volta Motors and several other automakers, stated that it is "technology-agnostic" and ready to supply the next generation of battery manufacturers. However, their long-term contracts with established players like Volta are now a subject of intense discussion among investors. The market is holding its breath to see if QuantumCharge Inc. can move from a successful prototype to mass production, a feat many have failed to achieve.
  "#
  }
}