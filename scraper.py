# -*- coding: utf-8 -*-
import os
# morph.io requires this db filename, but scraperwiki doesn't nicely
# expose a way to alter this. So we'll fiddle our environment ourselves
# before our pipeline modules load.
os.environ['SCRAPERWIKI_DATABASE_NAME'] = 'sqlite:///data.sqlite'
import re
import string
import scrapy
from scrapy.crawler import CrawlerProcess
import scraperwiki
SERIAL = 0
NAME_WITH_LINK = 1
PARTY = 2
AREA = 3
TERM = 4
# pylint: disable=C0301
class LokSabhaMembersSpider(scrapy.Spider):
    name = "lkmembers"
    honorific_prefixes = r"Shri|Dr\.|Prof\.|Smt\.|Kumari|Pandit|Sardar|Qazi|Mohammad|Maulana"
    honorific_suffixes = "Maulana"
    intext = None
    alp_at = 0
    base_url = 'http://164.100.47.194/Loksabha/Members/lokprev.aspx?search='

    def __init__(self, search=None, *args, **kwargs):
        super(LokSabhaMembersSpider, self).__init__(*args, **kwargs)
        self.intext = search

    def start_requests(self):
        if self.intext is None:
            yield self.get_request_for(self.alp_at)
        else:
            url = 'http://164.100.47.194/Loksabha/Members/lokprev.aspx'
            yield scrapy.Request(url=url, callback=self.search)

    def get_max(self, response):
        yield scrapy.FormRequest.from_response(
            response,
            formdata={'ctl00$ContentPlaceHolder1$drdpages': '5000'},
            callback=self.parse,
            errback=self.errback_next
            )

    def search(self, response):
        yield scrapy.FormRequest.from_response(
            response,
            formdata={
                'ctl00$ContentPlaceHolder1$txtSearch': self.intext,
                'ctl00$ContentPlaceHolder1$member': 'rdbtnName',
                'ctl00$ContentPlaceHolder1$hidTableCount': '',
                'ctl00$ContentPlaceHolder1$btnSearch' : ''},
            callback=self.parse,
            )

    def parse(self, response):
        members = response.xpath('//*[@class="member_list_table"]/tr')
        i = 0
        self.alp_at = self.alp_at + 1
        while i <= len(members):
            if i == len(members):
                if self.alp_at < len(string.ascii_uppercase) and self.intext is None:
                    yield self.get_request_for(self.alp_at)
            else:
                td = members[i].xpath('td')
                name_with_link_td = td[NAME_WITH_LINK]
                party_td = td[PARTY]
                area_td = td[AREA]
                term_td = td[TERM]
                namestring = name_with_link_td.xpath('normalize-space()').extract_first()
                honorific_prefix, honorific_suffix, first_name, middle_name, last_name, name = self.get_name(namestring)
                area, state = self.get_bracket_separated(area_td.xpath('normalize-space()'))
                url = response.urljoin(name_with_link_td.xpath('a/@href').extract_first())
                request = scrapy.Request(url=url, callback=self.redirect_to_old_new)
                request.meta['extra_data'] = {
                    'first_name': first_name,
                    'last_name' : last_name,
                    'middle_name' : middle_name,
                    'name' : name,
                    'honorific_prefix' : ";".join(honorific_prefix),
                    'honorific_suffix' : ";".join(honorific_suffix),
                    'party': party_td.xpath('normalize-space()').extract_first(),
                    'area': area,
                    'state' : state,
                    'term': term_td.xpath('normalize-space()').extract_first(),
                    'link': url,
                    'identifier_mpsno': name_with_link_td.xpath('a/@href').re(r'(?<=mpsno=)\d+')[0]
                    }
                yield request
            i = i + 1
    def errback_next(self, failure):
        if 'search' in failure.request.url and self.intext is None:
            self.alp_at = self.alp_at + 1
            if self.alp_at < len(string.ascii_uppercase):
                yield self.get_request_for(self.alp_at)

    def get_request_for(self, index):
        url = self.base_url + string.ascii_uppercase[index]
        return scrapy.Request(url=url, callback=self.get_max, errback=self.errback_next)

    def redirect_to_old_new(self, response):
        data_in = response.meta['extra_data']
        if 'mpsno' not in response.url:
             data_in = self.parse_old_members(response)
        else:
             data_in = self.parse_new_members(response)
        scraperwiki.sqlite.save(
        unique_keys=['identifier_mpsno'], data =  dict(data_in)
        )

    def parse_old_members(self, response):
        data = response.meta['extra_data']
        imgsrc = response.xpath('//*/img/@src').extract_first()
        data['image_src'] = response.urljoin(imgsrc)

        last_term = data['term'][len(data['term'])-1]

        if last_term == u"10":
            self.scrap_10(response, data)

        elif last_term == u"11" or last_term == u"12":
            self.scrap_11_12(response, data)

        else:
            self.scrap_all_others(response, data)
        return data

    def parse_new_members(self, response):
        data = response.meta['extra_data']
        imgsrc = response.xpath('//*/img[@id="ContentPlaceHolder1_Image1"]/@src').extract_first()
        data['image_src'] = response.urljoin(imgsrc)

        email_idsa = self.new_match_string(res=response, string="Email Address :", ext="td/text()").extract()
        if email_idsa is not None:
            data['email_ids'] = ';'.join((self.strip_and_join(lista=email_idsa, joinby=";")).split(' ')).replace("[AT]", "@").replace("[DOT]", ".").replace(',','')

        website = self.new_match_string(res=response, string="Website :", ext="td/a/@href").extract_first()
        if website is not None:
            data['website'] = website.replace("\r\n", '').replace(' ', '')

        data['father_name'] = self.ext_n_norm(self.new_match_string(res=response, string="Father\'s Name", ext="td"))
        data['mother_name'] = self.ext_n_norm(self.new_match_string(res=response, string="Mother\'s Name", ext="td"))
        data['date_of_birth'] = self.ext_n_norm(self.new_match_string(res=response, string="Date of Birth", ext="td"))
        data['place_of_birth'] = self.ext_n_norm(self.new_match_string(res=response, string="Place of Birth", ext="td"))
        data['maritial_status'] = self.ext_n_norm(self.new_match_string(res=response, string="Marital Status", ext="td"))
        data['spouse_name'] = self.ext_n_norm(self.new_match_string(res=response, string="Spouse\'s Name", ext="td"))
        data['special_interests'] = self.ext_n_norm(self.new_match_string(res=response, string="Special Interests", ext="tr", parent=True))
        data['other_info'] = self.ext_n_norm(self.new_match_string(res=response, string="Other Information", ext="tr", parent=True))
        data['favourite_past_time_and_recreation'] = self.ext_n_norm(self.new_match_string(res=response, string="Favourite Pastime and Recreation", ext="tr", parent=True))
        data['literary_artistic_scientific_accomplishments'] = self.ext_n_norm(self.new_match_string(res=response, string="Literary Artistic & Scientific Accomplishments", ext="tr", parent=True))
        data['books_published'] = self.ext_n_norm(self.new_match_string(res=response, string="Books Published", ext="tr", parent=True))
        data['sports_and_club'] = self.ext_n_norm(self.new_match_string(res=response, string="Sports and Clubs", ext="tr", parent=True))
        data['countries_visited'] = self.ext_n_norm(self.new_match_string(res=response, string="Countries Visited", ext="tr", parent=True))

        educationa = self.new_match_string(res=response, string="Educational", ext="td/text()").extract()
        data['education'] = self.strip_and_join(lista=educationa, joinby=";")
        positionsheld = response.xpath('//*[@id="ContentPlaceHolder1_Datagrid3"]/tr/td/font/table/tr/td/text()').extract()
        data['positions_held'] = self.strip_and_join(lista=positionsheld, joinby=";")
        professiona = self.new_match_string(res=response, string="Profession", ext="td/text()").extract()
        data['profession'] = self.strip_and_join(lista=professiona, joinby=";")

        paddress = ""
        caddress = ""
        for paddressa in self.new_match_string(res=response, string="Permanent Address", ext="td/table"):
            paddress = paddress + self.strip_and_join(lista=paddressa.xpath('tr/td/text()').extract(), joinby="") + ';'
        for caddressa in self.new_match_string(res=response, string="Present Address", ext="td/table"):
            caddress = caddress + self.strip_and_join(lista=caddressa.xpath('tr/td/text()').extract(), joinby="") + ';'
        data['present_address'] = caddress
        data['permanent_address'] = paddress

        data['fb_link'] = response.xpath('//*[@id="ContentPlaceHolder1_fblnk"]/@href').extract_first()
        data['twitter_link'] = response.xpath('//*[@id="ContentPlaceHolder1_twtrlnk"]/@href').extract_first()
        data['app_link'] = response.xpath('//*[@id="ContentPlaceHolder1_applink"]/@href').extract_first()

        return data

    def scrap_10(self, response, data):
        what = response.xpath('//table/tr/td')
        ihd = len(what) - 2
        itext = len(what) - 1
        if ihd <= 0:
            return
        i = 0
        j = 0
        hd_list = list(filter(None, what[ihd].xpath('*').xpath('normalize-space()').extract()))
        text_list = list(filter(None, what[itext].xpath('*').xpath('normalize-space()').extract()))
        while i < len(hd_list) and j < len(text_list):
            hd = hd_list[i]
            text = text_list[j]
            if hd == u"Positions held " or hd == u"Positions held":
                positionsheld = ''
                while True:
                    i = i+1
                    j = j+1
                    hd = hd_list[i]
                    text = text_list[j]
                    if re.search('[a-zA-Z]', hd) != None:
                        j = j+1
                        break
                    positionsheld = positionsheld + hd + '-' + text + ";"
                data['positions_held'] = positionsheld
            if len(text) < 2:
                i = i+1
                j = j+1
                continue
            if hd == u"Father's Name":
                data['father_name'] = text
            elif hd == u"Date of Birth":
                data['date_of_birth'] = text
            elif hd == u"Place of Birth":
                data['place_of_birth'] = text
            elif hd == u"Marital Status":
                data['maritial_status'] = text
            elif hd == u"Spouse's Name":
                data['spouse_name'] = text
            elif hd == u"Educational Qualifications":
                data['education'] = text
            elif hd == u"Profession":
                data['profession'] = text
            elif hd == u"Permanent Address":
                data['permanent_address'] = text
            elif hd == u"Present Address":
                data['present_address'] = text
            elif hd == u"Literary, Artistic and Scientific Accomplishments":
                data['literary_artistic_scientific_accomplishments'] = text
            elif hd == u"Social and Cultural Activities":
                data['social_activities'] = text
            elif hd == u"Favourite Pastime and Recreation":
                data['favourite_past_time_and_recreation'] = text
            elif hd == u"Sports and Clubs":
                data['sports_and_clubs'] = text
            elif hd == u"Countries visited":
                data['countries_visited'] = text
            elif hd == u"Other Information":
                data['other_info'] = text
            elif hd == u"Special Interests":
                data['special_interests'] = text
            i = i+1
            j = j+1

    def scrap_11_12(self, response, data):
        i = 0
        textlist = []
        temp = response.xpath("//body//text()")
        while i < len(temp):
            text = response.xpath('normalize-space("' + temp[i].extract().replace("\"", "&quot;") + '")').extract_first()
            if text:
                textlist.append(text)
            i = i+1
        i = 0
        while i < len(textlist):
            if textlist[i] == u"Father's Name":
                i = i+1
                if textlist[i] == u":":
                    i = i+1
                data[u'father_name'] = textlist[i]
            elif textlist[i] == u"Date of Birth":
                i = i+1
                if textlist[i] == u":":
                    i = i+1
                data[u'date_of_birth'] = textlist[i]
            elif textlist[i] == u"Place of Birth":
                i = i+1
                if textlist[i] == u":":
                    i = i+1
                data[u'place_of_birth'] = textlist[i]
            elif textlist[i] == u"Marital Status":
                i = i+1
                if textlist[i] == u":":
                    i = i+1
                data[u'maritial_status'] = textlist[i]
            elif textlist[i] == u"Spouse's Name":
                i = i+1
                if textlist[i] == u":":
                    i = i+1
                data[u'spouse_name'] = textlist[i]
            elif textlist[i] == u"Educational Qualifications":
                i = i+1
                if textlist[i] == u":":
                    i = i+1
                data[u'education'] = textlist[i]
            elif textlist[i] == u"Profession":
                i = i+1
                if textlist[i] == u":":
                    i = i+1
                data[u'profession'] = textlist[i]
            elif textlist[i] == u"Permanent Address":
                i = i+1
                if textlist[i] == u":":
                    i = i+1
                data[u'permanent_address'] = textlist[i]
            elif textlist[i] == u"Present Address":
                i = i+1
                if textlist[i] == u":":
                    i = i+1
                data[u'present_address'] = textlist[i]
            elif textlist[i] == u"Positions Held":
                i = i+1
                if textlist[i] == u":":
                    i = i+1
                data[u'positions_held'] = textlist[i]
            elif textlist[i] == u"Books Published":
                i = i+1
                if textlist[i] == u":":
                    i = i+1
                data[u'books_published'] = textlist[i]
            i = i+1
    def scrap_all_others(self, response, data):
        dobio = False
        for itext in response.xpath('//table/tr/td').xpath('*[normalize-space()]'):
            text = itext.xpath('normalize-space()').extract_first()
            if dobio:
                data['bio'] = text.strip()
                if len(data['bio']) < 17:
                    continue
                dobio = False
            if re.search('Members Bioprofile', text, re.I):
                dobio = True
            elif re.search('Permanent address', text, re.I):
                data['permanent_address'] = self.replace_and_return('Permanent address:', text)
                if data['permanent_address'] == text:
                    data['permanent_address'] = self.replace_and_return(r'Permanent address.', text).strip(u'—')
            elif re.search('Other Information', text, re.I):
                data['other_info'] = self.replace_and_return('Other Information:', text)
                if data['other_info'] == text:
                    data['other_info'] = self.replace_and_return(r'Other Information.', text).strip(u'—')
            elif re.search('Social activities', text, re.I):
                data['social_activities'] = self.replace_and_return('Social activities:', text)
                if data['social_activities'] == text:
                    data['social_activities'] = self.replace_and_return(r'Social activities.', text).strip(u'—')
            elif re.search('Travels Abroad', text, re.I):
                data['countries_visited'] = self.replace_and_return('Travels Abroad:', text)
                if data['countries_visited'] == text:
                    data['countries_visited'] = self.replace_and_return(r'Travels Abroad.', text).strip(u'—')
            elif re.search('Publications', text, re.I):
                data['books_published'] = self.replace_and_return('Publications:', text)
                if data['books_published'] == text:
                    data['books_published'] = self.replace_and_return(r'Publications.', text).strip(u'—')
            elif re.search('Favourite pastime and recreation', text, re.I):
                data['favourite_past_time_and_recreation'] = self.replace_and_return('Favourite pastime and recreation:', text)
                if data['favourite_past_time_and_recreation'] == text:
                    data['favourite_past_time_and_recreation'] = self.replace_and_return(r'Favourite pastime and recreation.', text).strip(u'—')
            elif re.search('Previous Membership', text, re.I):
                data['positions_held'] = self.replace_and_return('Previous Membership:', text)
                if data['positions_held'] == text:
                    data['positions_held'] = self.replace_and_return(r'Previous Membership.', text).strip(u'—')
            elif re.search('Hobbies', text, re.I):
                data['hobbies'] = self.replace_and_return('Hobbies:', text)
                if data['hobbies'] == text:
                    data['hobbies'] = self.replace_and_return(r'Hobbies.', text).strip(u'—')
            elif re.search('Sports and Clubs', text, re.I):
                data['sports_and_club'] = self.replace_and_return('Sports and Clubs:', text)
                if data['sports_and_club'] == text:
                    data['sports_and_club'] = self.replace_and_return(r'Sports and Clubs.', text).strip(u'—')

    def new_match_string(self, res, string, ext, parent=False):
        if parent:
            return res.xpath('//td[normalize-space(text())="' + string + '"]/parent::tr/following-sibling::' + ext)
        else:
            return res.xpath('//td[normalize-space(text())="' + string + '"]/following-sibling::' + ext)

    def ext_n_norm(self, data):
        return data.xpath('normalize-space()').extract_first()

    def save_data(self, dictdata):
        yield dictdata

    def get_bracket_separated(self, td):
        tmp = td.extract_first()
        first = ""
        second = ""
        lenh = len(tmp)
        fro = lenh - 1
        if lenh > 0 and tmp[fro] == u')':
            while tmp[fro] != u'(' and fro != 0:
                fro = fro - 1
            second = self.remove_brackets(tmp[fro:])
        tmp = td.re(r'.+(?=\()')
        if tmp:
            first = tmp[0].strip()
        else:
            first = td.extract_first()
        return first, second

    def get_name(self, namestring):
        namelist = namestring.split(',')
        firstname = lastname = middlename = honorificprefix = honorificsuffix = ""
        if len(namelist) > 1:
            honorificprefix, firstname = self.remove_get_honorific_prefix(instring=namelist[1])
        firstname = firstname.strip()
        if len(firstname) <= 0:
            firstname = namelist[0]
        else:
            honorificsuffix, lastname = self.remove_get_honorific_suffix(instring=namelist[0])
        namelist = firstname.split(' ')
        if len(namelist) > 1:
            firstname = namelist[0]
            if lastname:
                middlename = namelist[1]
            else:
                lastname = namelist[1]
        lastname = lastname.strip()
        namelist = lastname.split(' ')
        if len(namelist) > 1:
            lastname = namelist[1]
            if firstname:
                middlename = namelist[0]
            else:
                firstname = namelist[0]
        name = firstname.strip()
        if middlename:
            name = name + ' ' + middlename.strip()
        if lastname:
            name = name + ' ' + lastname.strip()
        return honorificprefix, honorificsuffix, firstname.strip(), middlename.strip(), lastname.strip(), name.strip()

    def remove_get_honorific_prefix(self, instring):
        honorific_prefix = re.findall('('+self.honorific_prefixes+')+', instring, re.I)
        name = ""
        if not honorific_prefix:
            name = instring
        else:
            tmp = re.search('\\b(?:(?!'+self.honorific_prefixes+')\\w).+', instring, re.I)
            if tmp != None:
                name = tmp.group()
        return honorific_prefix, name

    def remove_get_honorific_suffix(self, instring):
        honorific_suffix = re.findall('('+self.honorific_suffixes+')+', instring, re.I)
        name = ""
        if not honorific_suffix:
            name = instring
        else:
            tmp = re.search('\\b(?:(?!'+self.honorific_suffixes+')\\w).+', instring, re.I)
            if tmp != None:
                name = tmp.group()
        return honorific_suffix, name

    def strip_and_join(self, lista, joinby):
        j = ""
        for i in lista:
            j = j + i.strip() + joinby
        return j[0:-1]

    def remove_brackets(self, string):
        if string.startswith('(') and string.endswith(')'):
            return string[1:-1]
        else:
            return string

    def replace_and_return(self, replacestr, instr):
        return re.compile(re.escape(replacestr), re.IGNORECASE).sub('', instr).strip()
process = CrawlerProcess()
process.crawl(LokSabhaMembersSpider, terms=16)
process.start()
