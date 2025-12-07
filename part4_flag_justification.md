# Justification

## Flagging system overview
Our flagging system targets the SBU men's and women's basketball team by analyzing the players' accumulated acceleration load and left/right max force assymetry. These two metrics are specially important for the players in the basketball team because accumulated acceleration load is the neuromuscular stress that a player puts on their body during practice or game. Furthermore, left/right max force helps to identify assymetry levels between the two limbs, in the specific case of basketball, the left and right lower limbs will focused. These two metrics help to identify whether a player is at risk of injury or not.

## Accumulated Acceleration Load (AAL)

### Threshold Definition
- AAL value > 90th percentile of gender group

Monitoring external load during training and games can improve training performance and avoid injury risks. A 2022 study showed that performance efforts exceeding the 95th percentile in external load increased significant risks for injury (Carey et al., 2022). Although this does not match with that used in our flagging system and there are no standard percentiles used as thresholds, we wanted to to lower that percentile so that there is more precautious flagging. We want to ensure that players are identified earlier in the percentile rather than at the exact 95 percentile so that coaches can have more time to adjust training loads.

### Gender-specific rationale
There was also a separation for the 90th percentile threshold between the men's and women's basketball team becausae they have different physiologis which means they output different ranges of external load. By comparing athlete to the same gender helps to properly define the percentiles and analyze whether or not they hit the threshold within their group.

## Left/Right max force assymetry

### Threshold Definition
- ((Strong Side - Weak Side) / Strong Side) × 100% > 10%

Limb symmetry index is used to either determine that an athlete is at risk of injury or if they are already injured, whether or not they are ready to come back to train and perform. Parkinson et al. (2021) noted that there is no universal threshold for LSI and many sources that state the 10-15% threshold lack evidence in the foundation of the threshold. With that in mind, this we dicided to use 10% as the threshold with the same reason that we chose the 90th percentile external laod as the AAL threshold. We wanted more precaution for the athletes so we lowered the threshold. We wanted to ensure that we are able to flag players who are already have assymetries to be continuously monitored rather than letting them continue practicing and reaching the 15% LSI.

Left/Right assymetry is treated similarly for both women and men since the assymetry level is solely determined by the athlete's own LSI. Therefore, there was no grouping by gender for this threshold.