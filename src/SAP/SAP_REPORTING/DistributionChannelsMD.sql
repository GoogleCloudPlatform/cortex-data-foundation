#--
#--     https://www.apache.org/licenses/LICENSE-2.0
#--
#-- Unless required by applicable law or agreed to in writing, software
#-- distributed under the License is distributed on an "AS IS" BASIS,
#-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-- See the License for the specific language governing permissions and
#-- limitations under the License.

{% if sql_flavour == 'ecc' or sql_flavour == 'union' -%}
{% include './ecc/DistributionChannelsMD.sql' -%}
{% endif -%}

{% if sql_flavour == 'union' -%}
UNION ALL
{% endif -%}

{% if sql_flavour == 's4' or sql_flavour == 'union' -%}
{% include './s4/DistributionChannelsMD.sql' -%}
{% endif -%}

