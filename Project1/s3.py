{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "metadata": {},
   "outputs": [],
   "source": [
    "#Importing libraries\n",
    "import pandas as pd\n",
    "\n",
    "\n",
    "#setting display of column width to see more output\n",
    "pd.options.display.max_colwidth = 80\n",
    "\n",
    "#Setting output to ignore warnings\n",
    "import warnings\n",
    "warnings.filterwarnings('ignore')"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "The syntax for downloading the 7 Million Dataset from S3 bucket didn't work actually."
   ]
  },
  {
   "cell_type": "raw",
   "metadata": {},
   "source": [
    "#Downloading the 7+ Million Dataset from S3\n",
    "my_data = ''\n",
    "key = ''\n",
    "import boto3\n",
    "import botocore\n",
    "\n",
    "BUCKET_NAME = 'blossom-data-engs' # replace with your bucket name\n",
    "KEY = 'project1/free-7-million-company-dataset.zip' # replace with your object key\n",
    "\n",
    "s3 = boto3.resource('s3')\n",
    "\n",
    "try:\n",
    "    s3.Bucket(BUCKET_NAME).download_file(KEY, 'C:/Users/USER/Desktop/Blossom_Academy/companies_sorted.csv')\n",
    "except botocore.exceptions.ClientError as e:\n",
    "    if e.response['Error']['Code'] == \"404\":\n",
    "        print(\"The object does not exist.\")\n",
    "    else:\n",
    "        raise"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "### Reading the Dataset"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>Unnamed: 0</th>\n",
       "      <th>name</th>\n",
       "      <th>domain</th>\n",
       "      <th>year founded</th>\n",
       "      <th>industry</th>\n",
       "      <th>size range</th>\n",
       "      <th>locality</th>\n",
       "      <th>country</th>\n",
       "      <th>linkedin url</th>\n",
       "      <th>current employee estimate</th>\n",
       "      <th>total employee estimate</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>5872184</td>\n",
       "      <td>ibm</td>\n",
       "      <td>ibm.com</td>\n",
       "      <td>1911.0</td>\n",
       "      <td>information technology and services</td>\n",
       "      <td>10001+</td>\n",
       "      <td>new york, new york, united states</td>\n",
       "      <td>united states</td>\n",
       "      <td>linkedin.com/company/ibm</td>\n",
       "      <td>274047</td>\n",
       "      <td>716906</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>4425416</td>\n",
       "      <td>tata consultancy services</td>\n",
       "      <td>tcs.com</td>\n",
       "      <td>1968.0</td>\n",
       "      <td>information technology and services</td>\n",
       "      <td>10001+</td>\n",
       "      <td>bombay, maharashtra, india</td>\n",
       "      <td>india</td>\n",
       "      <td>linkedin.com/company/tata-consultancy-services</td>\n",
       "      <td>190771</td>\n",
       "      <td>341369</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>21074</td>\n",
       "      <td>accenture</td>\n",
       "      <td>accenture.com</td>\n",
       "      <td>1989.0</td>\n",
       "      <td>information technology and services</td>\n",
       "      <td>10001+</td>\n",
       "      <td>dublin, dublin, ireland</td>\n",
       "      <td>ireland</td>\n",
       "      <td>linkedin.com/company/accenture</td>\n",
       "      <td>190689</td>\n",
       "      <td>455768</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>3</th>\n",
       "      <td>2309813</td>\n",
       "      <td>us army</td>\n",
       "      <td>goarmy.com</td>\n",
       "      <td>1800.0</td>\n",
       "      <td>military</td>\n",
       "      <td>10001+</td>\n",
       "      <td>alexandria, virginia, united states</td>\n",
       "      <td>united states</td>\n",
       "      <td>linkedin.com/company/us-army</td>\n",
       "      <td>162163</td>\n",
       "      <td>445958</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>4</th>\n",
       "      <td>1558607</td>\n",
       "      <td>ey</td>\n",
       "      <td>ey.com</td>\n",
       "      <td>1989.0</td>\n",
       "      <td>accounting</td>\n",
       "      <td>10001+</td>\n",
       "      <td>london, greater london, united kingdom</td>\n",
       "      <td>united kingdom</td>\n",
       "      <td>linkedin.com/company/ernstandyoung</td>\n",
       "      <td>158363</td>\n",
       "      <td>428960</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "   Unnamed: 0                       name         domain  year founded  \\\n",
       "0     5872184                        ibm        ibm.com        1911.0   \n",
       "1     4425416  tata consultancy services        tcs.com        1968.0   \n",
       "2       21074                  accenture  accenture.com        1989.0   \n",
       "3     2309813                    us army     goarmy.com        1800.0   \n",
       "4     1558607                         ey         ey.com        1989.0   \n",
       "\n",
       "                              industry size range  \\\n",
       "0  information technology and services     10001+   \n",
       "1  information technology and services     10001+   \n",
       "2  information technology and services     10001+   \n",
       "3                             military     10001+   \n",
       "4                           accounting     10001+   \n",
       "\n",
       "                                 locality         country  \\\n",
       "0       new york, new york, united states   united states   \n",
       "1              bombay, maharashtra, india           india   \n",
       "2                 dublin, dublin, ireland         ireland   \n",
       "3     alexandria, virginia, united states   united states   \n",
       "4  london, greater london, united kingdom  united kingdom   \n",
       "\n",
       "                                     linkedin url  current employee estimate  \\\n",
       "0                        linkedin.com/company/ibm                     274047   \n",
       "1  linkedin.com/company/tata-consultancy-services                     190771   \n",
       "2                  linkedin.com/company/accenture                     190689   \n",
       "3                    linkedin.com/company/us-army                     162163   \n",
       "4              linkedin.com/company/ernstandyoung                     158363   \n",
       "\n",
       "   total employee estimate  \n",
       "0                   716906  \n",
       "1                   341369  \n",
       "2                   455768  \n",
       "3                   445958  \n",
       "4                   428960  "
      ]
     },
     "execution_count": 2,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "#Reading the file\n",
    "df = pd.read_csv('companies_sorted.csv')\n",
    "\n",
    "#Converting dataset into Dataframe\n",
    "data = pd.DataFrame(df)\n",
    "\n",
    "#Printing out the first 5 rows of the Dataframe\n",
    "data.head()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "Unnamed: 0                         0\n",
       "name                               3\n",
       "domain                       1650621\n",
       "year founded                 3606980\n",
       "industry                      290003\n",
       "size range                         0\n",
       "locality                     2508825\n",
       "country                      2349207\n",
       "linkedin url                       0\n",
       "current employee estimate          0\n",
       "total employee estimate            0\n",
       "dtype: int64"
      ]
     },
     "execution_count": 3,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "#Filtering out the columns with null values\n",
    "data.isnull().sum()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "metadata": {},
   "outputs": [],
   "source": [
    "#Filtering out companies without domain names\n",
    "output = data.name[data.domain.isnull()]"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "32                                                                             nhs\n",
      "57                                                ayatama energi, trisco nusantara\n",
      "59                                                    american center of krasnodar\n",
      "61                           promobroker agente de seguros y de fianzas s a de c v\n",
      "81                                                     verbum traducción y edición\n",
      "110                                                        ministerio de educación\n",
      "181                                                        lavoratore indipendente\n",
      "212                        bright futures, a college and career management company\n",
      "229                                                                          fedex\n",
      "264                                                             siemens healthcare\n",
      "301                                                               stay at home mom\n",
      "305                                                            amazon web services\n",
      "314                                                                   cvs pharmacy\n",
      "325                                                                 wipro infotech\n",
      "328                                                                        samsung\n",
      "334        biocolloids fate and transport in environmental systems, network biomet\n",
      "338                                                                  comcast cable\n",
      "351                        \"cecaci\"  centro de capacitacion comercial e industrial\n",
      "388                                                            indite technologies\n",
      "389                                                                mahindra satyam\n",
      "390                                                                coldwell banker\n",
      "417                                                 korea facilitators association\n",
      "429                                                       barclays investment bank\n",
      "449                                                          promsvyaznedvizhimost\n",
      "467                                                               bayer healthcare\n",
      "482                           departament d'ensenyament - generalitat de catalunya\n",
      "496                                                             accenture in india\n",
      "524                                                        standard chartered bank\n",
      "569                                                     us army corps of engineers\n",
      "570                                                     united states army reserve\n",
      "                                            ...                                   \n",
      "7173347                                             rockin s construction co. inc.\n",
      "7173353                                                              tin roof cafe\n",
      "7173356                                                 the jacob's bakery limited\n",
      "7173357                                                          prklin automation\n",
      "7173358                                                             ghezzi masonry\n",
      "7173360                                                          memci car service\n",
      "7173369                                                                     winove\n",
      "7173374                                             fundación consejo españa-india\n",
      "7173375                     banco ing – nationale nederland - país vasco - espanha\n",
      "7173382                                                      sherman graphics inc.\n",
      "7173385                                             the consumer satisfaction team\n",
      "7173388                                                   montes multiple services\n",
      "7173391                                                       hoffman orthodontics\n",
      "7173392                                                        open-space-projects\n",
      "7173395                                                   champion trace golf club\n",
      "7173396                                                    national payment center\n",
      "7173398                                                grund chiropractic wellness\n",
      "7173399                                                                   snertill\n",
      "7173400                                                      grand street creative\n",
      "7173403                                                                  oar group\n",
      "7173407                                                              smc precision\n",
      "7173409                                                builders showcase interiors\n",
      "7173412                                                            si modo limited\n",
      "7173413                                              a w hargrove insurance agency\n",
      "7173414                                                  humphreys accessories llc\n",
      "7173418                                                      koop media management\n",
      "7173419                                                            la belle miette\n",
      "7173420                                                        inkredible printing\n",
      "7173423                                                 catholic bishop of chicago\n",
      "7173424                                                        medexo robotics ltd\n",
      "Name: name, Length: 1650621, dtype: object\n"
     ]
    }
   ],
   "source": [
    "print(output).format(__name__)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "### Writing the data into file json format"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "metadata": {},
   "outputs": [],
   "source": [
    "\n",
    "output.to_json(r'C:/Users/USER/Desktop/Blossom_Academy/free-7-million-company-dataset-json.gzip.json', compression = 'gzip').format(__name__)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "### Writing the dataset into Parquet file format"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "metadata": {},
   "outputs": [],
   "source": [
    "output = pd.DataFrame(output)\n",
    "output.to_parquet(\"free-7-million-company-dataset.parquet\", compression=\"gzip\").format(__name__)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "### Writing file format in AVRO"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "import spark\n",
    "output.write.avro('C:/Users/USER/Desktop/Blossom_Academy/free-7-million-company-dataset.avro')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.7.3"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 2
}
