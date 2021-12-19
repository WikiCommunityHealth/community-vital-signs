# Community Vital Signs
=================================

The [__Community Vital Signs__](https://meta.wikimedia.org/wiki/Community_Health_Metrics#Community_Vital_Signs) is a research project whose purpose is to measure the state of growth and renewal of the active community.

We propose the creation of '''6 indicators''' that we call Vital Signs. In Medicine, vital signs indicate the status of the body’s vital (life-sustaining) functions. These measurements are taken to help assess the general physical health of a person, give clues to possible diseases, and show progress toward recovery.

In the case of Wikipedia, Vital Signs are related to the community capacity and function to grow or renew itself. Three of them are focused on the entire group of “active editors” creating content: __retention__, __stability__, and __balance__; the other four are related to more specific community functions: __admins__, __specialists__, and __global community participation__. We believe that obtaining the current “active community capacities” in these areas can constitute a valuable reference point to plan to guarantee “openness” in these areas, and at the same time, to observe growth and renewal, and foresee and prevent future risks (e.g., bus factor).

We are currently working on a live dashboard to consult the Vital Signs. 


## Code and Data: Editors and Web Databases
* `vital_signs.py` it retrieves the data from Wikimedia dumps [MediaWiki History Dump](https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Edits/MediaWiki_history) into the database vital_signs_editors.db. It also creates and populates the database [vital_signs_web.db](https://wikicommunityhealth.wmcloud.org/datasets/vital_signs_web.db), which is used by the website.


## Technology
The Vital Signs method is build with:
- [Python 3](https://www.python.org/download/releases/3.0/) - To manage the data.
- [Sqlite 3](https://www.sqlite.org/) - To store the data.
- [Scikit-learn](https://scikit-learn.org) - To classify the data.


* Presentation Vital Signs (2021)
There are multiple presentations on how to apply the Vital Signs. For example, this one in [WikiArabia presentation 2021](https://upload.wikimedia.org/wikipedia/commons/transcoded/e/e9/Measuring_Arabic_Wikipedia_Community_Health_video_presentation_in_Wikiarabia.webm/Measuring_Arabic_Wikipedia_Community_Health_video_presentation_in_Wikiarabia.webm.480p.vp9.webm).

Likewise, you can consult the [Report on Polish Wikipedia Vital Signs](https://meta.wikimedia.org/wiki/Community_Health_Metrics/Polish_Wikipedia_Report).

## Community
Get involved in Vital Signs development and find tasks to do in [Get involved page](https://meta.wikimedia.org/wiki/Community_Health_Metrics#Get_Involved).

## Copyright
All data, charts, and other content is available under the [Creative Commons CC0 dedication](https://creativecommons.org/publicdomain/zero/1.0/).
